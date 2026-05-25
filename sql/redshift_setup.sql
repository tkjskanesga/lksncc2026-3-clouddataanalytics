-- Task 13: Redshift Setup SQL
-- Create staging tables, COPY commands, and materialized views

-- Create schemas
CREATE SCHEMA IF NOT EXISTS staging;
CREATE SCHEMA IF NOT EXISTS analytics;
CREATE SCHEMA IF NOT EXISTS reporting;

-- ============================================
-- STAGING TABLES
-- ============================================

-- Staging table: transactions
CREATE TABLE IF NOT EXISTS staging.transactions (
    transaction_id VARCHAR(36) NOT NULL,
    order_date DATE NOT NULL,
    seller_id VARCHAR(36),
    buyer_id VARCHAR(36),
    product_id VARCHAR(36),
    category VARCHAR(100),
    province VARCHAR(100),
    quantity INTEGER,
    unit_price DECIMAL(12,2),
    total_amount DECIMAL(12,2),
    payment_method VARCHAR(50),
    status VARCHAR(20),
    created_at TIMESTAMP
)
DISTKEY (transaction_id)
SORTKEY (order_date);

-- Staging table: shipments
CREATE TABLE IF NOT EXISTS staging.shipments (
    shipment_id VARCHAR(36) NOT NULL,
    transaction_id VARCHAR(36),
    courier VARCHAR(50),
    origin_province VARCHAR(100),
    dest_province VARCHAR(100),
    weight_kg DECIMAL(6,2),
    shipping_cost DECIMAL(10,2),
    pickup_date DATE,
    estimated_arrival DATE,
    actual_arrival DATE,
    status VARCHAR(20)
)
DISTKEY (transaction_id)
SORTKEY (pickup_date);

-- Staging table: sellers
CREATE TABLE IF NOT EXISTS staging.sellers (
    seller_id VARCHAR(36) NOT NULL,
    seller_name VARCHAR(255),
    tier VARCHAR(20),
    province VARCHAR(100),
    join_date DATE,
    total_products INTEGER,
    rating DECIMAL(3,2),
    is_official BOOLEAN
)
DISTKEY (seller_id)
SORTKEY (join_date);

-- ============================================
-- COPY COMMANDS (to be executed separately)
-- ============================================

-- COPY staging.transactions
-- FROM 's3://nusa-processed-data-{ACCOUNT_ID}/transactions/'
-- IAM_ROLE 'arn:aws:iam::{ACCOUNT_ID}:role/LabRole'
-- FORMAT AS PARQUET;

-- COPY staging.shipments
-- FROM 's3://nusa-processed-data-{ACCOUNT_ID}/shipments/'
-- IAM_ROLE 'arn:aws:iam::{ACCOUNT_ID}:role/LabRole'
-- FORMAT AS PARQUET;

-- COPY staging.sellers
-- FROM 's3://nusa-processed-data-{ACCOUNT_ID}/sellers/'
-- IAM_ROLE 'arn:aws:iam::{ACCOUNT_ID}:role/LabRole'
-- FORMAT AS PARQUET;

-- ============================================
-- MATERIALIZED VIEW: Daily Summary
-- ============================================

CREATE MATERIALIZED VIEW reporting.mv_daily_summary AS
SELECT
    t.order_date,
    t.category,
    t.province AS buyer_province,
    t.payment_method,
    COUNT(DISTINCT t.transaction_id) AS order_count,
    COUNT(DISTINCT t.buyer_id) AS unique_buyers,
    SUM(t.total_amount) AS gmv,
    SUM(CASE WHEN t.status = 'COMPLETED' THEN t.total_amount ELSE 0 END) AS completed_gmv,
    AVG(t.total_amount) AS aov,
    ROUND(
        SUM(CASE WHEN t.status = 'COMPLETED' THEN t.total_amount ELSE 0 END) 
        / NULLIF(SUM(t.total_amount), 0) * 100, 2
    ) AS completion_rate_pct,
    AVG(DATEDIFF(day, sh.pickup_date, sh.actual_arrival)) AS avg_delivery_days,
    SUM(sh.shipping_cost) AS total_shipping_cost,
    MAX(t.created_at) AS last_updated
FROM staging.transactions t
LEFT JOIN staging.shipments sh ON t.transaction_id = sh.transaction_id
WHERE t.status IN ('COMPLETED', 'CANCELLED', 'PENDING', 'REFUNDED')
GROUP BY
    t.order_date,
    t.category,
    t.province,
    t.payment_method;

-- ============================================
-- ANALYTICS VIEWS
-- ============================================

-- View: Top sellers by revenue
CREATE VIEW analytics.v_top_sellers AS
SELECT
    t.seller_id,
    s.seller_name,
    s.tier,
    s.province,
    COUNT(DISTINCT t.transaction_id) AS total_transactions,
    SUM(t.total_amount) AS total_revenue,
    AVG(t.total_amount) AS avg_transaction_value,
    COUNT(DISTINCT t.buyer_id) AS unique_customers,
    s.rating AS seller_rating
FROM staging.transactions t
JOIN staging.sellers s ON t.seller_id = s.seller_id
WHERE t.status = 'COMPLETED'
GROUP BY
    t.seller_id,
    s.seller_name,
    s.tier,
    s.province,
    s.rating;

-- View: Category performance
CREATE VIEW analytics.v_category_performance AS
SELECT
    category,
    COUNT(DISTINCT transaction_id) AS order_count,
    COUNT(DISTINCT buyer_id) AS unique_buyers,
    SUM(total_amount) AS gmv,
    AVG(total_amount) AS aov,
    COUNT(DISTINCT seller_id) AS seller_count,
    ROUND(
        SUM(CASE WHEN status = 'COMPLETED' THEN total_amount ELSE 0 END) 
        / NULLIF(SUM(total_amount), 0) * 100, 2
    ) AS completion_rate_pct
FROM staging.transactions
GROUP BY category;

-- View: Province heatmap
CREATE VIEW analytics.v_province_heatmap AS
SELECT
    province,
    COUNT(DISTINCT transaction_id) AS order_count,
    COUNT(DISTINCT buyer_id) AS unique_buyers,
    SUM(total_amount) AS gmv,
    AVG(total_amount) AS aov
FROM staging.transactions
WHERE status = 'COMPLETED'
GROUP BY province;
