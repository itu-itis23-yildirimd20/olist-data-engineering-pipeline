# YZV322 — Olist E-Commerce Data Engineering Pipeline

An end-to-end data engineering pipeline built on the Olist Brazilian e-commerce open dataset. The entire stack runs with Docker, integrating 6 different tools.

---

## Architecture

```
CSV Files (9 datasets)
        │
        ▼
  ┌─────────────┐
  │  PostgreSQL │  raw → staging → warehouse (star schema)
  └──────┬──────┘
         │ Orchestrated by Airflow DAG
         ▼
  ┌──────────────────┐
  │  Elasticsearch   │  3 indices (orders, products, categories)
  └──────┬───────────┘
         │
    ┌────┴────┐
    │  Kibana │  Dashboard & visualization
    └─────────┘
         │
  ┌──────┴──────┐
  │   FastAPI   │  Analytics API (port 8000)
  └─────────────┘
         │
  ┌──────┴──────┐
  │    NiFi     │  Data flow management (port 9090)
  └─────────────┘
```

---

## Services & Ports

| Service       | URL                        | Credentials              |
|---------------|----------------------------|--------------------------|
| Airflow       | http://localhost:8080      | admin / admin            |
| NiFi          | http://localhost:9090      | admin / adminpassword123 |
| Kibana        | http://localhost:5601      | —                        |
| FastAPI Docs  | http://localhost:8000/docs | —                        |
| Elasticsearch | http://localhost:9200      | —                        |
| PostgreSQL    | localhost:5432             | olist / olist            |

---

## Requirements

- Docker Desktop or Docker Engine + Docker Compose
- At least 8 GB RAM (required for Elasticsearch)
- 10 GB free disk space

---

## Setup & Launch

### Step 1 — Clone the repository

```bash
git clone <repo-url>
cd project
```

### Step 2 — Verify the data folder

The `data/` directory must contain the following CSV files:

```
data/
├── olist_customers_dataset.csv
├── olist_orders_dataset.csv
├── olist_order_items_dataset.csv
├── olist_order_payments_dataset.csv
├── olist_order_reviews_dataset.csv
├── olist_products_dataset.csv
├── olist_sellers_dataset.csv
├── olist_geolocation_dataset.csv
└── product_category_name_translation.csv
```

### Step 3 — Create Airflow directories

```bash
mkdir -p airflow/logs airflow/plugins
```

### Step 4 — Start infrastructure first (PostgreSQL + Elasticsearch)

```bash
docker compose up -d postgres elasticsearch
```

### Step 5 — Wait for PostgreSQL to be ready

```bash
until docker compose exec postgres pg_isready -U postgres; do sleep 2; done
```

### Step 6 — Wait for Elasticsearch to be ready

```bash
until curl -sf http://localhost:9200/_cluster/health; do sleep 3; done
```

### Step 7 — Apply single-node Elasticsearch setting (fixes Kibana green index issue)

```bash
curl -X PUT "http://localhost:9200/_template/single_node" \
  -H 'Content-Type: application/json' \
  -d '{"index_patterns":["*"],"settings":{"number_of_replicas":0}}'
```

### Step 8 — Start all remaining services

```bash
docker compose up -d
```

### Step 9 — Check service status

```bash
docker compose ps
```

All services should show `running` or `healthy`. Kibana may take 2–3 minutes to fully initialize.

---

## Running the Pipeline

Once all services are up, trigger the Airflow DAG either from the UI or the terminal:

**From the UI:**
1. Open `http://localhost:8080` → admin / admin
2. Find `olist_pipeline` in the DAGs list
3. Click the **▶ Trigger DAG** button
4. Watch tasks turn green in Graph view

**From the terminal:**
```bash
docker compose exec airflow-scheduler airflow dags trigger olist_pipeline
```

The DAG runs 4 tasks in order:

```
load_raw_data → transform_to_staging → load_warehouse → index_to_elasticsearch
```

| Task                   | Description                                     |
|------------------------|-------------------------------------------------|
| load_raw_data          | Loads 7 CSV files into the raw schema           |
| transform_to_staging   | Type casting, cleaning, writes to staging       |
| load_warehouse         | Populates dim/fact tables, refreshes views      |
| index_to_elasticsearch | Indexes 3 materialized views into Elasticsearch |

---

## PostgreSQL — 3-Layer Architecture

### Layers

| Layer     | Description                                        |
|-----------|----------------------------------------------------|
| raw       | Raw data loaded directly from CSV files (all TEXT) |
| staging   | Type-cast and cleaned data                         |
| warehouse | Star schema with dimension and fact tables         |

### Warehouse Tables

**Dimension tables:**
- `dim_customers` — customer information
- `dim_products` — product and category details
- `dim_sellers` — seller information
- `dim_date` — date dimension

**Fact tables:**
- `fact_orders` — order facts
- `fact_order_items` — order line items
- `fact_payments` — payment records

**Materialized views:**
- `vw_revenue_by_state` — revenue aggregated by state
- `vw_top_categories` — category performance metrics
- `vw_seller_performance` — seller metrics

### Running SQL from the Terminal

```bash
docker compose exec postgres psql -U olist -d olist
```

**Example queries:**

```sql
-- Row counts across all three layers
SELECT 'raw.orders' AS layer, COUNT(*) FROM raw.orders
UNION ALL SELECT 'staging.orders', COUNT(*) FROM staging.orders
UNION ALL SELECT 'warehouse.fact_orders', COUNT(*) FROM warehouse.fact_orders;

-- Top 5 categories by revenue
SELECT
  p.product_category_name_english AS category,
  COUNT(oi.item_key)               AS order_count,
  ROUND(SUM(oi.price)::numeric, 2) AS total_revenue
FROM warehouse.fact_order_items oi
JOIN warehouse.dim_products p ON oi.product_key = p.product_key
GROUP BY 1
ORDER BY 3 DESC
LIMIT 5;

-- Orders by state
SELECT
  c.customer_state AS state,
  COUNT(*)         AS orders
FROM warehouse.fact_orders fo
JOIN warehouse.dim_customers c ON fo.customer_key = c.customer_key
GROUP BY 1
ORDER BY 2 DESC
LIMIT 10;

-- Order status distribution
SELECT order_status, COUNT(*) AS count
FROM warehouse.fact_orders
GROUP BY 1
ORDER BY 2 DESC;
```

---

## Elasticsearch + Kibana — Search & Visualization

### Check Indices

```bash
curl http://localhost:9200/_cat/indices?v
```

After the Airflow pipeline completes, the following indices will exist:
- `olist_categories` — category performance metrics
- `olist_revenue_by_state` — revenue aggregated by state
- `olist_sellers` — seller performance metrics

### Building a Kibana Dashboard

Open `http://localhost:5601`.

**Step 1 — Create Data Views:**
1. Left menu → **Stack Management** → **Data Views**
2. Click **Create data view**
3. Name: `olist_categories`, Index pattern: `olist_categories` → **Save**
4. Repeat for `olist_revenue_by_state` and `olist_sellers`

**Step 2 — Create a Dashboard:**
Left menu → **Dashboards** → **Create dashboard**

**Visualization 1: Revenue by Category (Horizontal Bar)**
- Create visualization → Bar horizontal
- Index: `olist_categories`
- Y-axis: category name field (keyword)
- X-axis: Sum of revenue field
- Save as: "Revenue by Category"

**Visualization 2: Revenue by State (Bar Chart)**
- Create visualization → Bar vertical
- Index: `olist_revenue_by_state`
- X-axis: state field (keyword)
- Y-axis: Sum of revenue field
- Save as: "Revenue by State"

**Visualization 3: Seller Performance (Table)**
- Create visualization → Table
- Index: `olist_sellers`
- Rows: seller city, seller state
- Metrics: order count, total revenue
- Save as: "Seller Performance"

**Step 3:** Add all visualizations to the dashboard → **Save** → "Olist E-Commerce Dashboard"

---

## FastAPI — Analytics API

Swagger UI is available at `http://localhost:8000/docs`.

### Endpoints

| Endpoint                      | Description                                  |
|-------------------------------|----------------------------------------------|
| GET `/health`                 | Service health check                         |
| GET `/api/stats/summary`      | Global KPIs (orders, revenue, customers)     |
| GET `/api/revenue-by-state`   | Revenue by state with optional year filter   |
| GET `/api/top-categories`     | Top-selling product categories               |
| GET `/api/seller-performance` | Seller performance metrics                   |
| GET `/api/orders/{order_id}`  | Full order detail with items and payments    |
| GET `/api/search/categories`  | Full-text category search via Elasticsearch  |
| GET `/api/search/sellers`     | Seller search with state filter              |

### Example Requests

```bash
curl http://localhost:8000/api/stats/summary
curl http://localhost:8000/api/top-categories
curl "http://localhost:8000/api/search/categories?q=beauty"
```

---

## Apache NiFi — Data Flow Management

`http://localhost:9090` → admin / adminpassword123

Included as a course requirement. NiFi provides a visual interface for building and managing data flow pipelines.

---

## Stopping the Stack

```bash
docker compose down
```

To also delete all data volumes:
```bash
docker compose down -v
```

---

## Project Structure

```
project/
├── docker-compose.yml        # 7 service definitions
├── .env                      # AIRFLOW_UID
├── data/                     # 9 Olist CSV datasets
├── postgres/
│   └── init/
│       ├── 01_create_users_and_databases.sql
│       ├── 02_raw_schema.sql
│       ├── 03_staging_schema.sql
│       └── 04_warehouse_schema.sql
├── airflow/
│   ├── Dockerfile
│   ├── requirements.txt
│   └── dags/
│       └── olist_pipeline.py
├── fastapi/
│   ├── Dockerfile
│   ├── main.py
│   └── requirements.txt
└── nifi/
```
