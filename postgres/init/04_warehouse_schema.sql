\c olist

CREATE SCHEMA IF NOT EXISTS warehouse;

-- Dimension Tables
CREATE TABLE IF NOT EXISTS warehouse.dim_date (
    date_key SERIAL PRIMARY KEY,
    full_date DATE UNIQUE NOT NULL,
    year INTEGER,
    quarter INTEGER,
    month INTEGER,
    month_name VARCHAR(20),
    week INTEGER,
    day_of_month INTEGER,
    day_of_week INTEGER,
    day_name VARCHAR(20),
    is_weekend BOOLEAN
);

CREATE TABLE IF NOT EXISTS warehouse.dim_customers (
    customer_key SERIAL PRIMARY KEY,
    customer_id VARCHAR(50) UNIQUE NOT NULL,
    customer_unique_id VARCHAR(50),
    customer_zip_code_prefix VARCHAR(10),
    customer_city VARCHAR(100),
    customer_state VARCHAR(5)
);

CREATE TABLE IF NOT EXISTS warehouse.dim_sellers (
    seller_key SERIAL PRIMARY KEY,
    seller_id VARCHAR(50) UNIQUE NOT NULL,
    seller_zip_code_prefix VARCHAR(10),
    seller_city VARCHAR(100),
    seller_state VARCHAR(5)
);

CREATE TABLE IF NOT EXISTS warehouse.dim_products (
    product_key SERIAL PRIMARY KEY,
    product_id VARCHAR(50) UNIQUE NOT NULL,
    product_category_name VARCHAR(100),
    product_category_name_english VARCHAR(100),
    product_name_lenght INTEGER,
    product_description_lenght INTEGER,
    product_photos_qty INTEGER,
    product_weight_g NUMERIC(10,2),
    product_length_cm NUMERIC(10,2),
    product_height_cm NUMERIC(10,2),
    product_width_cm NUMERIC(10,2)
);

-- Fact Tables
CREATE TABLE IF NOT EXISTS warehouse.fact_orders (
    order_key SERIAL PRIMARY KEY,
    order_id VARCHAR(50) UNIQUE NOT NULL,
    customer_key INTEGER REFERENCES warehouse.dim_customers(customer_key),
    purchase_date_key INTEGER REFERENCES warehouse.dim_date(date_key),
    order_status VARCHAR(30),
    order_purchase_timestamp TIMESTAMP,
    order_approved_at TIMESTAMP,
    order_delivered_carrier_date TIMESTAMP,
    order_delivered_customer_date TIMESTAMP,
    order_estimated_delivery_date TIMESTAMP
);

CREATE TABLE IF NOT EXISTS warehouse.fact_order_items (
    item_key SERIAL PRIMARY KEY,
    order_id VARCHAR(50) NOT NULL,
    order_item_id INTEGER,
    product_key INTEGER REFERENCES warehouse.dim_products(product_key),
    seller_key INTEGER REFERENCES warehouse.dim_sellers(seller_key),
    shipping_date_key INTEGER REFERENCES warehouse.dim_date(date_key),
    shipping_limit_date TIMESTAMP,
    price NUMERIC(10,2),
    freight_value NUMERIC(10,2),
    total_value NUMERIC(10,2) GENERATED ALWAYS AS (price + freight_value) STORED
);

CREATE TABLE IF NOT EXISTS warehouse.fact_payments (
    payment_key SERIAL PRIMARY KEY,
    order_id VARCHAR(50) NOT NULL,
    payment_sequential INTEGER,
    payment_type VARCHAR(30),
    payment_installments INTEGER,
    payment_value NUMERIC(10,2)
);

-- Analytical Views
CREATE MATERIALIZED VIEW IF NOT EXISTS warehouse.vw_revenue_by_state AS
SELECT
    dc.customer_state,
    dd.year,
    dd.month,
    dd.month_name,
    SUM(fp.payment_value) AS total_revenue,
    COUNT(DISTINCT fo.order_id) AS total_orders
FROM warehouse.fact_orders fo
JOIN warehouse.dim_customers dc ON fo.customer_key = dc.customer_key
JOIN warehouse.dim_date dd ON fo.purchase_date_key = dd.date_key
JOIN warehouse.fact_payments fp ON fo.order_id = fp.order_id
WHERE fo.order_status = 'delivered'
GROUP BY dc.customer_state, dd.year, dd.month, dd.month_name
ORDER BY dd.year, dd.month, total_revenue DESC;

CREATE MATERIALIZED VIEW IF NOT EXISTS warehouse.vw_top_categories AS
SELECT
    dp.product_category_name_english AS category,
    COUNT(DISTINCT foi.order_id) AS total_orders,
    SUM(foi.price) AS total_revenue,
    ROUND(AVG(foi.price), 2) AS avg_price,
    COUNT(foi.item_key) AS total_items_sold
FROM warehouse.fact_order_items foi
JOIN warehouse.dim_products dp ON foi.product_key = dp.product_key
WHERE dp.product_category_name_english IS NOT NULL
GROUP BY dp.product_category_name_english
ORDER BY total_revenue DESC;

CREATE MATERIALIZED VIEW IF NOT EXISTS warehouse.vw_seller_performance AS
SELECT
    ds.seller_id,
    ds.seller_city,
    ds.seller_state,
    COUNT(DISTINCT foi.order_id) AS total_orders,
    SUM(foi.price) AS total_revenue,
    ROUND(AVG(foi.price), 2) AS avg_order_value,
    COUNT(foi.item_key) AS total_items_sold
FROM warehouse.fact_order_items foi
JOIN warehouse.dim_sellers ds ON foi.seller_key = ds.seller_key
GROUP BY ds.seller_id, ds.seller_city, ds.seller_state
ORDER BY total_revenue DESC;

GRANT USAGE ON SCHEMA warehouse TO olist;
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA warehouse TO olist;
