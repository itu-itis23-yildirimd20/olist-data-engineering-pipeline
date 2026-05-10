# NiFi Flow Setup

NiFi is used for initial CSV ingestion. After the stack starts, access NiFi at:
https://localhost:8443/nifi

Credentials: admin / adminpassword123

## Flow Design

Create one flow group per CSV file with this processor chain:

1. **GetFile** — reads CSV from `/opt/nifi/data/`
2. **SplitRecord** — splits into individual records (CSV Reader / CSV Writer)
3. **PutDatabaseRecord** — inserts into `raw.<table>` in PostgreSQL

## JDBC Connection Pool

- Driver: PostgreSQL JDBC (postgresql-42.x.jar)
- URL: `jdbc:postgresql://postgres:5432/olist`
- User: `olist` / Password: `olist`

## Files to ingest

| File | Target Table |
|------|-------------|
| olist_orders_dataset.csv | raw.orders |
| olist_order_items_dataset.csv | raw.order_items |
| olist_customers_dataset.csv | raw.customers |
| olist_sellers_dataset.csv | raw.sellers |
| olist_products_dataset.csv | raw.products |
| olist_order_payments_dataset.csv | raw.payments |
| product_category_name_translation.csv | raw.category_translation |

> Note: The Airflow DAG also handles raw loading via Python. NiFi provides an alternative GUI-based ingestion path.
