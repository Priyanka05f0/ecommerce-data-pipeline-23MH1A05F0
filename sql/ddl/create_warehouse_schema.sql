-- ============================
-- CREATE WAREHOUSE SCHEMA
-- ============================
CREATE SCHEMA IF NOT EXISTS warehouse;

-- ============================
-- DIMENSION: DATE
-- ============================
CREATE TABLE IF NOT EXISTS warehouse.dim_date (
    date_key INTEGER PRIMARY KEY,
    full_date DATE NOT NULL,
    year INTEGER,
    quarter INTEGER,
    month INTEGER,
    day INTEGER,
    month_name VARCHAR(20),
    day_name VARCHAR(20),
    week_of_year INTEGER,
    is_weekend BOOLEAN
);

-- ============================
-- DIMENSION: CUSTOMERS (SCD TYPE 2)
-- ============================
CREATE TABLE IF NOT EXISTS warehouse.dim_customers (
    customer_key SERIAL PRIMARY KEY,
    customer_id VARCHAR(20),
    full_name VARCHAR(200),
    email VARCHAR(150),
    city VARCHAR(100),
    state VARCHAR(100),
    country VARCHAR(100),
    age_group VARCHAR(20),
    effective_date DATE,
    end_date DATE,
    is_current BOOLEAN
);

-- ============================
-- DIMENSION: PRODUCTS (SCD TYPE 2)
-- ============================
CREATE TABLE IF NOT EXISTS warehouse.dim_products (
    product_key SERIAL PRIMARY KEY,
    product_id VARCHAR(20),
    product_name VARCHAR(150),
    category VARCHAR(100),
    sub_category VARCHAR(100),
    brand VARCHAR(100),
    price DECIMAL(10,2),
    cost DECIMAL(10,2),
    price_range VARCHAR(20),
    effective_date DATE,
    end_date DATE,
    is_current BOOLEAN
);

-- ============================
-- DIMENSION: PAYMENT METHOD
-- ============================
CREATE TABLE IF NOT EXISTS warehouse.dim_payment_method (
    payment_method_key SERIAL PRIMARY KEY,
    payment_method_name VARCHAR(50),
    payment_type VARCHAR(20)
);

-- ============================
-- FACT TABLE: SALES
-- ============================
CREATE TABLE IF NOT EXISTS warehouse.fact_sales (
    sales_key BIGSERIAL PRIMARY KEY,
    date_key INTEGER REFERENCES warehouse.dim_date(date_key),
    customer_key INTEGER REFERENCES warehouse.dim_customers(customer_key),
    product_key INTEGER REFERENCES warehouse.dim_products(product_key),
    payment_method_key INTEGER REFERENCES warehouse.dim_payment_method(payment_method_key),
    transaction_id VARCHAR(20),
    quantity INTEGER,
    unit_price DECIMAL(10,2),
    discount_amount DECIMAL(10,2),
    line_total DECIMAL(12,2),
    profit DECIMAL(12,2),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
