from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
import psycopg2
import os

default_args = {
    'owner': 'olist',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

PG_CONN = {
    'host': os.environ.get('POSTGRES_HOST', 'postgres'),
    'port': int(os.environ.get('POSTGRES_PORT', 5432)),
    'dbname': os.environ.get('POSTGRES_DB', 'olist'),
    'user': os.environ.get('POSTGRES_USER', 'olist'),
    'password': os.environ.get('POSTGRES_PASSWORD', 'olist'),
}

DATA_DIR = '/opt/airflow/data'


def get_conn():
    return psycopg2.connect(**PG_CONN)


def load_raw_data():
    import csv
    conn = get_conn()
    cur = conn.cursor()

    files = {
        'orders': 'olist_orders_dataset.csv',
        'order_items': 'olist_order_items_dataset.csv',
        'customers': 'olist_customers_dataset.csv',
        'sellers': 'olist_sellers_dataset.csv',
        'products': 'olist_products_dataset.csv',
        'payments': 'olist_order_payments_dataset.csv',
        'category_translation': 'product_category_name_translation.csv',
    }

    for table, filename in files.items():
        path = os.path.join(DATA_DIR, filename)
        cur.execute(f'TRUNCATE TABLE raw.{table}')
        with open(path, 'r', encoding='utf-8-sig') as f:
            reader = csv.reader(f)
            headers = next(reader)
            placeholders = ','.join(['%s'] * len(headers))
            for row in reader:
                cur.execute(f'INSERT INTO raw.{table} VALUES ({placeholders})', row)
        conn.commit()
        print(f'Loaded raw.{table}')

    cur.close()
    conn.close()


def transform_to_staging():
    conn = get_conn()
    cur = conn.cursor()

    cur.execute('TRUNCATE TABLE staging.customers CASCADE')
    cur.execute("""
        INSERT INTO staging.customers
        SELECT DISTINCT ON (customer_id)
            customer_id,
            customer_unique_id,
            customer_zip_code_prefix,
            LOWER(TRIM(customer_city)),
            UPPER(TRIM(customer_state))
        FROM raw.customers
        WHERE customer_id IS NOT NULL AND customer_id <> ''
    """)

    cur.execute('TRUNCATE TABLE staging.sellers CASCADE')
    cur.execute("""
        INSERT INTO staging.sellers
        SELECT DISTINCT ON (seller_id)
            seller_id,
            seller_zip_code_prefix,
            LOWER(TRIM(seller_city)),
            UPPER(TRIM(seller_state))
        FROM raw.sellers
        WHERE seller_id IS NOT NULL AND seller_id <> ''
    """)

    cur.execute('TRUNCATE TABLE staging.category_translation CASCADE')
    cur.execute("""
        INSERT INTO staging.category_translation
        SELECT DISTINCT product_category_name, product_category_name_english
        FROM raw.category_translation
        WHERE product_category_name IS NOT NULL AND product_category_name <> ''
        ON CONFLICT (product_category_name) DO NOTHING
    """)

    cur.execute('TRUNCATE TABLE staging.products CASCADE')
    cur.execute("""
        INSERT INTO staging.products
        SELECT DISTINCT ON (product_id)
            product_id,
            product_category_name,
            NULLIF(product_name_lenght, '')::INTEGER,
            NULLIF(product_description_lenght, '')::INTEGER,
            NULLIF(product_photos_qty, '')::INTEGER,
            NULLIF(product_weight_g, '')::NUMERIC,
            NULLIF(product_length_cm, '')::NUMERIC,
            NULLIF(product_height_cm, '')::NUMERIC,
            NULLIF(product_width_cm, '')::NUMERIC
        FROM raw.products
        WHERE product_id IS NOT NULL AND product_id <> ''
    """)

    cur.execute('TRUNCATE TABLE staging.orders CASCADE')
    cur.execute("""
        INSERT INTO staging.orders
        SELECT DISTINCT ON (order_id)
            order_id,
            customer_id,
            order_status,
            NULLIF(order_purchase_timestamp, '')::TIMESTAMP,
            NULLIF(order_approved_at, '')::TIMESTAMP,
            NULLIF(order_delivered_carrier_date, '')::TIMESTAMP,
            NULLIF(order_delivered_customer_date, '')::TIMESTAMP,
            NULLIF(order_estimated_delivery_date, '')::TIMESTAMP
        FROM raw.orders
        WHERE order_id IS NOT NULL AND order_id <> ''
    """)

    cur.execute('TRUNCATE TABLE staging.order_items CASCADE')
    cur.execute("""
        INSERT INTO staging.order_items
        SELECT
            order_id,
            order_item_id::INTEGER,
            product_id,
            seller_id,
            NULLIF(shipping_limit_date, '')::TIMESTAMP,
            NULLIF(price, '')::NUMERIC,
            NULLIF(freight_value, '')::NUMERIC
        FROM raw.order_items
        WHERE order_id IS NOT NULL AND order_id <> ''
        ON CONFLICT (order_id, order_item_id) DO NOTHING
    """)

    cur.execute('TRUNCATE TABLE staging.payments CASCADE')
    cur.execute("""
        INSERT INTO staging.payments
        SELECT
            order_id,
            payment_sequential::INTEGER,
            payment_type,
            NULLIF(payment_installments, '')::INTEGER,
            NULLIF(payment_value, '')::NUMERIC
        FROM raw.payments
        WHERE order_id IS NOT NULL AND order_id <> ''
        ON CONFLICT (order_id, payment_sequential) DO NOTHING
    """)

    conn.commit()
    cur.close()
    conn.close()
    print('Staging transformation complete')


def load_warehouse():
    conn = get_conn()
    cur = conn.cursor()

    # Populate dim_date from order purchase dates
    cur.execute("""
        INSERT INTO warehouse.dim_date (full_date, year, quarter, month, month_name, week, day_of_month, day_of_week, day_name, is_weekend)
        SELECT DISTINCT
            d::DATE,
            EXTRACT(YEAR FROM d)::INTEGER,
            EXTRACT(QUARTER FROM d)::INTEGER,
            EXTRACT(MONTH FROM d)::INTEGER,
            TO_CHAR(d, 'Month'),
            EXTRACT(WEEK FROM d)::INTEGER,
            EXTRACT(DAY FROM d)::INTEGER,
            EXTRACT(DOW FROM d)::INTEGER,
            TO_CHAR(d, 'Day'),
            EXTRACT(DOW FROM d) IN (0, 6)
        FROM (
            SELECT DISTINCT order_purchase_timestamp::DATE AS d FROM staging.orders WHERE order_purchase_timestamp IS NOT NULL
            UNION
            SELECT DISTINCT shipping_limit_date::DATE FROM staging.order_items WHERE shipping_limit_date IS NOT NULL
        ) dates
        ON CONFLICT (full_date) DO NOTHING
    """)

    cur.execute("""
        INSERT INTO warehouse.dim_customers (customer_id, customer_unique_id, customer_zip_code_prefix, customer_city, customer_state)
        SELECT customer_id, customer_unique_id, customer_zip_code_prefix, customer_city, customer_state
        FROM staging.customers
        ON CONFLICT (customer_id) DO UPDATE SET
            customer_city = EXCLUDED.customer_city,
            customer_state = EXCLUDED.customer_state
    """)

    cur.execute("""
        INSERT INTO warehouse.dim_sellers (seller_id, seller_zip_code_prefix, seller_city, seller_state)
        SELECT seller_id, seller_zip_code_prefix, seller_city, seller_state
        FROM staging.sellers
        ON CONFLICT (seller_id) DO UPDATE SET
            seller_city = EXCLUDED.seller_city,
            seller_state = EXCLUDED.seller_state
    """)

    cur.execute("""
        INSERT INTO warehouse.dim_products (product_id, product_category_name, product_category_name_english,
            product_name_lenght, product_description_lenght, product_photos_qty,
            product_weight_g, product_length_cm, product_height_cm, product_width_cm)
        SELECT
            p.product_id,
            p.product_category_name,
            ct.product_category_name_english,
            p.product_name_lenght,
            p.product_description_lenght,
            p.product_photos_qty,
            p.product_weight_g,
            p.product_length_cm,
            p.product_height_cm,
            p.product_width_cm
        FROM staging.products p
        LEFT JOIN staging.category_translation ct ON p.product_category_name = ct.product_category_name
        ON CONFLICT (product_id) DO UPDATE SET
            product_category_name_english = EXCLUDED.product_category_name_english
    """)

    cur.execute("""
        INSERT INTO warehouse.fact_orders (order_id, customer_key, purchase_date_key, order_status,
            order_purchase_timestamp, order_approved_at, order_delivered_carrier_date,
            order_delivered_customer_date, order_estimated_delivery_date)
        SELECT
            o.order_id,
            dc.customer_key,
            dd.date_key,
            o.order_status,
            o.order_purchase_timestamp,
            o.order_approved_at,
            o.order_delivered_carrier_date,
            o.order_delivered_customer_date,
            o.order_estimated_delivery_date
        FROM staging.orders o
        JOIN warehouse.dim_customers dc ON o.customer_id = dc.customer_id
        LEFT JOIN warehouse.dim_date dd ON o.order_purchase_timestamp::DATE = dd.full_date
        ON CONFLICT (order_id) DO NOTHING
    """)

    cur.execute("""
        INSERT INTO warehouse.fact_order_items (order_id, order_item_id, product_key, seller_key,
            shipping_date_key, shipping_limit_date, price, freight_value)
        SELECT
            oi.order_id,
            oi.order_item_id,
            dp.product_key,
            ds.seller_key,
            dd.date_key,
            oi.shipping_limit_date,
            oi.price,
            oi.freight_value
        FROM staging.order_items oi
        LEFT JOIN warehouse.dim_products dp ON oi.product_id = dp.product_id
        LEFT JOIN warehouse.dim_sellers ds ON oi.seller_id = ds.seller_id
        LEFT JOIN warehouse.dim_date dd ON oi.shipping_limit_date::DATE = dd.full_date
        ON CONFLICT DO NOTHING
    """)

    cur.execute("""
        INSERT INTO warehouse.fact_payments (order_id, payment_sequential, payment_type, payment_installments, payment_value)
        SELECT order_id, payment_sequential, payment_type, payment_installments, payment_value
        FROM staging.payments
        ON CONFLICT DO NOTHING
    """)

    # Refresh materialized views
    cur.execute('REFRESH MATERIALIZED VIEW warehouse.vw_revenue_by_state')
    cur.execute('REFRESH MATERIALIZED VIEW warehouse.vw_top_categories')
    cur.execute('REFRESH MATERIALIZED VIEW warehouse.vw_seller_performance')

    conn.commit()
    cur.close()
    conn.close()
    print('Warehouse load complete')


def _bulk_index(es, index_name, cols, rows):
    from decimal import Decimal
    body = []
    for row in rows:
        body.append({'index': {'_index': index_name}})
        doc = {}
        for k, v in zip(cols, row):
            if isinstance(v, Decimal):
                v = float(v)
            doc[k] = v
        body.append(doc)
    if body:
        es.bulk(body=body, request_timeout=120)


def index_to_elasticsearch():
    from elasticsearch import Elasticsearch

    es_host = os.environ.get('ES_HOST', 'elasticsearch')
    es_port = int(os.environ.get('ES_PORT', 9200))
    es = Elasticsearch(f'http://{es_host}:{es_port}', request_timeout=120)

    conn = get_conn()
    cur = conn.cursor()

    # Index top categories
    cur.execute('SELECT category, total_orders, total_revenue, avg_price, total_items_sold FROM warehouse.vw_top_categories')
    rows = cur.fetchall()
    cols = ['category', 'total_orders', 'total_revenue', 'avg_price', 'total_items_sold']
    es.indices.delete(index='olist_categories', ignore_unavailable=True)
    _bulk_index(es, 'olist_categories', cols, rows)

    # Index revenue by state
    cur.execute('SELECT customer_state, year, month, month_name, total_revenue, total_orders FROM warehouse.vw_revenue_by_state')
    rows = cur.fetchall()
    cols = ['customer_state', 'year', 'month', 'month_name', 'total_revenue', 'total_orders']
    es.indices.delete(index='olist_revenue_by_state', ignore_unavailable=True)
    _bulk_index(es, 'olist_revenue_by_state', cols, rows)

    # Index seller performance
    cur.execute('SELECT seller_id, seller_city, seller_state, total_orders, total_revenue, avg_order_value, total_items_sold FROM warehouse.vw_seller_performance LIMIT 1000')
    rows = cur.fetchall()
    cols = ['seller_id', 'seller_city', 'seller_state', 'total_orders', 'total_revenue', 'avg_order_value', 'total_items_sold']

    es.indices.delete(index='olist_sellers', ignore_unavailable=True)
    _bulk_index(es, 'olist_sellers', cols, rows)

    cur.close()
    conn.close()
    print('Elasticsearch indexing complete')


with DAG(
    'olist_pipeline',
    default_args=default_args,
    description='End-to-end Olist data pipeline',
    schedule_interval='@daily',
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=['olist'],
) as dag:

    task_load_raw = PythonOperator(
        task_id='load_raw_data',
        python_callable=load_raw_data,
    )

    task_transform_staging = PythonOperator(
        task_id='transform_to_staging',
        python_callable=transform_to_staging,
    )

    task_load_warehouse = PythonOperator(
        task_id='load_warehouse',
        python_callable=load_warehouse,
    )

    task_index_es = PythonOperator(
        task_id='index_to_elasticsearch',
        python_callable=index_to_elasticsearch,
    )

    task_load_raw >> task_transform_staging >> task_load_warehouse >> task_index_es
