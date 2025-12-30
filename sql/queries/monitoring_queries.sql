-- 1. Data Freshness
SELECT
    'staging' AS layer,
    MAX(loaded_at) AS latest_record
FROM staging.transactions
UNION ALL
SELECT
    'production',
    MAX(created_at)
FROM production.transactions
UNION ALL
SELECT
    'warehouse',
    MAX(created_at)
FROM warehouse.fact_sales;

-- 2. Volume Trend (last 30 days)
SELECT
    transaction_date,
    COUNT(*) AS daily_count
FROM production.transactions
WHERE transaction_date >= CURRENT_DATE - INTERVAL '30 days'
GROUP BY transaction_date
ORDER BY transaction_date;

-- 3. Data Quality Checks
SELECT
    COUNT(*) FILTER (WHERE product_id IS NULL) AS null_product_ids,
    COUNT(*) FILTER (WHERE customer_id IS NULL) AS null_customer_ids
FROM production.transactions;

-- 4. Pipeline Execution History
SELECT
    start_time,
    end_time,
    status,
    total_duration_seconds
FROM pipeline_execution_log
ORDER BY start_time DESC
LIMIT 10;

-- 5. Database Statistics
SELECT
    relname AS table_name,
    n_live_tup AS row_count
FROM pg_stat_user_tables;
