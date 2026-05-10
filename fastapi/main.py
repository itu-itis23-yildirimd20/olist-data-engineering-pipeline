from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
import psycopg2
import psycopg2.extras
from elasticsearch import Elasticsearch
import os
from typing import Optional

app = FastAPI(title='Olist Analytics API', version='1.0.0')

app.add_middleware(
    CORSMiddleware,
    allow_origins=['*'],
    allow_methods=['*'],
    allow_headers=['*'],
)

PG_CONN = {
    'host': os.environ.get('POSTGRES_HOST', 'postgres'),
    'port': int(os.environ.get('POSTGRES_PORT', 5432)),
    'dbname': os.environ.get('POSTGRES_DB', 'olist'),
    'user': os.environ.get('POSTGRES_USER', 'olist'),
    'password': os.environ.get('POSTGRES_PASSWORD', 'olist'),
}

ES_HOST = os.environ.get('ES_HOST', 'elasticsearch')
ES_PORT = int(os.environ.get('ES_PORT', 9200))


def get_pg():
    return psycopg2.connect(**PG_CONN)


def get_es():
    return Elasticsearch(f'http://{ES_HOST}:{ES_PORT}')


@app.get('/health')
def health():
    return {'status': 'ok'}


@app.get('/api/revenue-by-state')
def revenue_by_state(state: Optional[str] = None, year: Optional[int] = None):
    conn = get_pg()
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    query = 'SELECT * FROM warehouse.vw_revenue_by_state WHERE 1=1'
    params = []
    if state:
        query += ' AND customer_state = %s'
        params.append(state.upper())
    if year:
        query += ' AND year = %s'
        params.append(year)
    query += ' ORDER BY year, month'
    cur.execute(query, params)
    rows = cur.fetchall()
    cur.close()
    conn.close()
    return [dict(r) for r in rows]


@app.get('/api/top-categories')
def top_categories(limit: int = Query(10, ge=1, le=100)):
    conn = get_pg()
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    cur.execute('SELECT * FROM warehouse.vw_top_categories LIMIT %s', (limit,))
    rows = cur.fetchall()
    cur.close()
    conn.close()
    return [dict(r) for r in rows]


@app.get('/api/seller-performance')
def seller_performance(
    state: Optional[str] = None,
    limit: int = Query(20, ge=1, le=500)
):
    conn = get_pg()
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    query = 'SELECT * FROM warehouse.vw_seller_performance WHERE 1=1'
    params = []
    if state:
        query += ' AND seller_state = %s'
        params.append(state.upper())
    query += ' ORDER BY total_revenue DESC LIMIT %s'
    params.append(limit)
    cur.execute(query, params)
    rows = cur.fetchall()
    cur.close()
    conn.close()
    return [dict(r) for r in rows]


@app.get('/api/orders/{order_id}')
def get_order(order_id: str):
    conn = get_pg()
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    cur.execute("""
        SELECT fo.*, dc.customer_city, dc.customer_state
        FROM warehouse.fact_orders fo
        JOIN warehouse.dim_customers dc ON fo.customer_key = dc.customer_key
        WHERE fo.order_id = %s
    """, (order_id,))
    order = cur.fetchone()
    if not order:
        raise HTTPException(status_code=404, detail='Order not found')

    cur.execute("""
        SELECT foi.*, dp.product_category_name_english, ds.seller_city, ds.seller_state
        FROM warehouse.fact_order_items foi
        LEFT JOIN warehouse.dim_products dp ON foi.product_key = dp.product_key
        LEFT JOIN warehouse.dim_sellers ds ON foi.seller_key = ds.seller_key
        WHERE foi.order_id = %s
    """, (order_id,))
    items = cur.fetchall()

    cur.execute('SELECT * FROM warehouse.fact_payments WHERE order_id = %s', (order_id,))
    payments = cur.fetchall()

    cur.close()
    conn.close()
    return {
        'order': dict(order),
        'items': [dict(i) for i in items],
        'payments': [dict(p) for p in payments],
    }


@app.get('/api/search/categories')
def search_categories(q: str = Query(..., min_length=1)):
    es = get_es()
    result = es.search(index='olist_categories', body={
        'query': {'match': {'category': {'query': q, 'fuzziness': 'AUTO'}}},
        'size': 10,
    })
    return [hit['_source'] for hit in result['hits']['hits']]


@app.get('/api/search/sellers')
def search_sellers(state: Optional[str] = None, limit: int = 10):
    es = get_es()
    query = {'match_all': {}} if not state else {'match': {'seller_state': state.upper()}}
    result = es.search(index='olist_sellers', body={'query': query, 'size': limit})
    return [hit['_source'] for hit in result['hits']['hits']]


@app.get('/api/stats/summary')
def summary():
    conn = get_pg()
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    cur.execute("""
        SELECT
            (SELECT COUNT(*) FROM warehouse.fact_orders) AS total_orders,
            (SELECT COUNT(*) FROM warehouse.dim_customers) AS total_customers,
            (SELECT COUNT(*) FROM warehouse.dim_sellers) AS total_sellers,
            (SELECT COUNT(*) FROM warehouse.dim_products) AS total_products,
            (SELECT COALESCE(SUM(payment_value), 0) FROM warehouse.fact_payments) AS total_revenue,
            (SELECT COUNT(DISTINCT order_status) FROM warehouse.fact_orders) AS distinct_statuses
    """)
    row = cur.fetchone()
    cur.close()
    conn.close()
    return dict(row)
