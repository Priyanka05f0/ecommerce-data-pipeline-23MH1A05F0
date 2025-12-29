-- =========================================================
-- ANALYTICAL QUERIES USING DATA WAREHOUSE (STAR SCHEMA)
-- Schema: warehouse
-- Fact Table: fact_sales
-- =========================================================


-- =========================================================
-- Query 1: Top 10 Products by Revenue
-- Objective: Identify best-selling products
-- =========================================================
SELECT
    p.product_name,
    p.category,
    SUM(f.line_total) AS total_revenue,
    SUM(f.quantity) AS units_sold,
    AVG(f.unit_price) AS avg_price
FROM warehouse.fact_sales f
JOIN warehouse.dim_products p
    ON f.product_key = p.product_key
GROUP BY p.product_name, p.category
ORDER BY total_revenue DESC
LIMIT 10;


-- =========================================================
-- Query 2: Monthly Sales Trend
-- Objective: Analyze revenue over time
-- =========================================================
SELECT
    d.year,
    d.month,
    SUM(f.line_total) AS total_revenue,
    COUNT(DISTINCT f.transaction_id) AS total_transactions,
    AVG(f.line_total) AS average_order_value,
    COUNT(DISTINCT f.customer_key) AS unique_customers
FROM warehouse.fact_sales f
JOIN warehouse.dim_date d
    ON f.date_key = d.date_key
GROUP BY d.year, d.month
ORDER BY d.year, d.month;


-- =========================================================
-- Query 3: Customer Segmentation Analysis
-- Objective: Group customers by spending behavior
-- =========================================================
WITH customer_totals AS (
    SELECT
        customer_key,
        SUM(line_total) AS total_spent,
        AVG(line_total) AS avg_transaction_value
    FROM warehouse.fact_sales
    GROUP BY customer_key
)
SELECT
    CASE
        WHEN total_spent < 1000 THEN '0-1000'
        WHEN total_spent < 5000 THEN '1000-5000'
        WHEN total_spent < 10000 THEN '5000-10000'
        ELSE '10000+'
    END AS spending_segment,
    COUNT(*) AS customer_count,
    SUM(total_spent) AS total_revenue,
    AVG(avg_transaction_value) AS avg_transaction_value
FROM customer_totals
GROUP BY spending_segment;


-- =========================================================
-- Query 4: Category Performance
-- Objective: Compare performance across product categories
-- =========================================================
SELECT
    p.category,
    SUM(f.line_total) AS total_revenue,
    SUM(f.profit) AS total_profit,
    (SUM(f.profit) / SUM(f.line_total)) * 100 AS profit_margin_pct,
    SUM(f.quantity) AS units_sold
FROM warehouse.fact_sales f
JOIN warehouse.dim_products p
    ON f.product_key = p.product_key
GROUP BY p.category;


-- =========================================================
-- Query 5: Payment Method Distribution
-- Objective: Understand payment preferences
-- =========================================================
SELECT
    pm.payment_method_name AS payment_method,
    COUNT(*) AS transaction_count,
    SUM(f.line_total) AS total_revenue,
    COUNT(*) * 100.0 / SUM(COUNT(*)) OVER () AS pct_of_transactions,
    SUM(f.line_total) * 100.0 / SUM(SUM(f.line_total)) OVER () AS pct_of_revenue
FROM warehouse.fact_sales f
JOIN warehouse.dim_payment_method pm
    ON f.payment_method_key = pm.payment_method_key
GROUP BY pm.payment_method_name;


-- =========================================================
-- Query 6: Geographic Analysis
-- Objective: Identify high-revenue locations
-- =========================================================
SELECT
    c.state,
    SUM(f.line_total) AS total_revenue,
    COUNT(DISTINCT f.customer_key) AS total_customers,
    AVG(f.line_total) AS avg_revenue_per_customer
FROM warehouse.fact_sales f
JOIN warehouse.dim_customers c
    ON f.customer_key = c.customer_key
GROUP BY c.state;


-- =========================================================
-- Query 7: Customer Lifetime Value (CLV)
-- Objective: Analyze customer value and tenure
-- =========================================================
SELECT
    c.customer_id,
    c.full_name,
    SUM(f.line_total) AS total_spent,
    COUNT(DISTINCT f.transaction_id) AS transaction_count,
    CURRENT_DATE - c.effective_date AS days_since_registration,
    AVG(f.line_total) AS avg_order_value
FROM warehouse.fact_sales f
JOIN warehouse.dim_customers c
    ON f.customer_key = c.customer_key
GROUP BY c.customer_id, c.full_name, c.effective_date;


-- =========================================================
-- Query 8: Product Profitability Analysis
-- Objective: Identify most profitable products
-- =========================================================
SELECT
    p.product_name,
    p.category,
    SUM(f.profit) AS total_profit,
    (SUM(f.profit) / SUM(f.line_total)) * 100 AS profit_margin,
    SUM(f.line_total) AS revenue,
    SUM(f.quantity) AS units_sold
FROM warehouse.fact_sales f
JOIN warehouse.dim_products p
    ON f.product_key = p.product_key
GROUP BY p.product_name, p.category
ORDER BY total_profit DESC;


-- =========================================================
-- Query 9: Day of Week Sales Pattern
-- Objective: Identify temporal sales trends
-- =========================================================
SELECT
    d.day_name,
    AVG(f.line_total) AS avg_daily_revenue,
    COUNT(DISTINCT f.transaction_id) AS avg_daily_transactions,
    SUM(f.line_total) AS total_revenue
FROM warehouse.fact_sales f
JOIN warehouse.dim_date d
    ON f.date_key = d.date_key
GROUP BY d.day_name;


-- =========================================================
-- Query 10: Discount Impact Analysis
-- Objective: Analyze discount effectiveness
-- =========================================================
SELECT
    CASE
        WHEN discount_amount = 0 THEN '0%'
        WHEN discount_amount < 0.1 * unit_price * quantity THEN '1-10%'
        WHEN discount_amount < 0.25 * unit_price * quantity THEN '11-25%'
        WHEN discount_amount < 0.5 * unit_price * quantity THEN '26-50%'
        ELSE '50%+'
    END AS discount_range,
    COUNT(*) AS total_orders,
    SUM(quantity) AS total_quantity_sold,
    SUM(line_total) AS total_revenue,
    AVG(line_total) AS avg_line_total
FROM warehouse.fact_sales
GROUP BY discount_range;
