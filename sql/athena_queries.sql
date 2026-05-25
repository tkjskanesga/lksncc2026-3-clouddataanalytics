-- =============================================================================
-- Athena Named Queries — NusaCommerce Analytics Platform
-- =============================================================================
-- Database: nusa-database
-- Region: us-east-1
-- Workgroup: nusa-workgroup
-- =============================================================================

-- ============================================================================
-- Query 1: nusa-daily-gmv
-- Deskripsi: Daily GMV per kategori produk
-- Tujuan: Memantau performa penjualan harian per kategori
-- ============================================================================
SELECT
    order_date,
    category,
    COUNT(DISTINCT transaction_id)          AS total_orders,
    COUNT(DISTINCT buyer_id)                AS unique_buyers,
    SUM(total_amount)                       AS gmv_idr,
    AVG(total_amount)                       AS avg_order_value,
    SUM(CASE WHEN status = 'COMPLETED' 
             THEN total_amount ELSE 0 END)  AS completed_gmv,
    ROUND(
        SUM(CASE WHEN status = 'COMPLETED' 
                 THEN total_amount ELSE 0 END) 
        / NULLIF(SUM(total_amount), 0) * 100, 2
    )                                       AS completion_rate_pct
FROM "nusa-database"."transactions"
WHERE order_date BETWEEN DATE '2024-01-01' AND DATE '2024-01-31'
GROUP BY order_date, category
ORDER BY order_date, gmv_idr DESC;

-- ============================================================================
-- Query 2: nusa-top-sellers
-- Deskripsi: Top 10 seller by revenue
-- Tujuan: Identifikasi seller dengan performa terbaik
-- ============================================================================
SELECT
    t.seller_id,
    s.seller_name,
    s.tier,
    s.province                              AS seller_province,
    COUNT(DISTINCT t.transaction_id)        AS total_transactions,
    SUM(t.total_amount)                     AS total_revenue,
    AVG(t.total_amount)                     AS avg_transaction_value,
    COUNT(DISTINCT t.buyer_id)              AS unique_customers,
    ROUND(AVG(s.rating), 2)                 AS seller_rating
FROM "nusa-database"."transactions" t
JOIN "nusa-database"."sellers" s 
    ON t.seller_id = s.seller_id
WHERE t.status = 'COMPLETED'
GROUP BY t.seller_id, s.seller_name, s.tier, s.province
ORDER BY total_revenue DESC
LIMIT 10;

-- ============================================================================
-- Query 3: nusa-category-performance
-- Deskripsi: Performa per kategori produk
-- Tujuan: Analisis mendalam tentang performa setiap kategori
-- ============================================================================
SELECT
    category,
    COUNT(DISTINCT transaction_id)          AS total_orders,
    COUNT(DISTINCT seller_id)               AS unique_sellers,
    COUNT(DISTINCT buyer_id)                AS unique_buyers,
    SUM(total_amount)                       AS total_gmv,
    AVG(total_amount)                       AS avg_order_value,
    MIN(total_amount)                       AS min_order_value,
    MAX(total_amount)                       AS max_order_value,
    ROUND(
        SUM(CASE WHEN status = 'COMPLETED' 
                 THEN total_amount ELSE 0 END) 
        / NULLIF(SUM(total_amount), 0) * 100, 2
    )                                       AS completion_rate_pct
FROM "nusa-database"."transactions"
GROUP BY category
ORDER BY total_gmv DESC;

-- ============================================================================
-- Query 4: nusa-province-heatmap
-- Deskripsi: Distribusi GMV per provinsi pembeli
-- Tujuan: Visualisasi geografis performa penjualan
-- ============================================================================
SELECT
    province,
    COUNT(DISTINCT transaction_id)          AS total_orders,
    COUNT(DISTINCT buyer_id)                AS unique_buyers,
    SUM(total_amount)                       AS total_gmv,
    AVG(total_amount)                       AS avg_order_value,
    ROUND(
        SUM(total_amount) 
        / (SELECT SUM(total_amount) FROM "nusa-database"."transactions") * 100, 2
    )                                       AS gmv_percentage
FROM "nusa-database"."transactions"
WHERE status = 'COMPLETED'
GROUP BY province
ORDER BY total_gmv DESC;

-- ============================================================================
-- Query 5: nusa-funnel-analysis
-- Deskripsi: Conversion funnel analysis dari page view hingga purchase
-- Tujuan: Analisis perilaku pengguna dan conversion rate di setiap tahap
-- ============================================================================
WITH funnel_events AS (
    SELECT
        DATE(event_timestamp)               AS event_date,
        platform,
        COUNT(DISTINCT CASE WHEN event_type = 'PAGE_VIEW' 
              THEN session_id END)          AS page_views,
        COUNT(DISTINCT CASE WHEN event_type = 'ADD_TO_CART' 
              THEN session_id END)          AS add_to_cart,
        COUNT(DISTINCT CASE WHEN event_type = 'CHECKOUT' 
              THEN session_id END)          AS checkouts,
        COUNT(DISTINCT CASE WHEN event_type = 'PURCHASE' 
              THEN session_id END)          AS purchases
    FROM "nusa-database"."user_events"
    GROUP BY DATE(event_timestamp), platform
)
SELECT
    event_date,
    platform,
    page_views,
    add_to_cart,
    checkouts,
    purchases,
    ROUND(add_to_cart * 100.0 / NULLIF(page_views, 0), 2)  AS view_to_cart_pct,
    ROUND(checkouts * 100.0 / NULLIF(add_to_cart, 0), 2)   AS cart_to_checkout_pct,
    ROUND(purchases * 100.0 / NULLIF(checkouts, 0), 2)     AS checkout_to_purchase_pct,
    ROUND(purchases * 100.0 / NULLIF(page_views, 0), 2)    AS overall_conversion_pct
FROM funnel_events
ORDER BY event_date DESC, platform;

-- =============================================================================
-- Catatan Penggunaan
-- =============================================================================
-- 1. Semua query menggunakan database "nusa-database" yang dibuat di Task 5
-- 2. Query dijalankan di workgroup "nusa-workgroup" dengan limit 1 GB per query
-- 3. Hasil query disimpan di s3://nusa-curated-data-{accountid}/athena-results/
-- 4. Encryption: SSE_S3 (AES-256)
-- 5. Untuk menjalankan query individual, gunakan Athena Console atau AWS CLI:
--    aws athena start-query-execution \
--      --query-string "SELECT COUNT(*) FROM \"nusa-database\".\"transactions\"" \
--      --query-execution-context "Database=nusa-database" \
--      --result-configuration "OutputLocation=s3://nusa-curated-data-{accountid}/athena-results/" \
--      --work-group nusa-workgroup \
--      --region us-east-1
-- =============================================================================

