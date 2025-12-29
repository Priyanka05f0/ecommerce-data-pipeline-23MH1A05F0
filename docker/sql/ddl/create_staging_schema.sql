-- ===============================
-- Create staging schema
-- ===============================
CREATE SCHEMA IF NOT EXISTS staging;

-- ===============================
-- Customers
-- ===============================
CREATE TABLE IF NOT EXISTS staging.customers (
    customer_id VARCHAR(20) PRIMARY KEY,
    first_name VARCHAR(100),
    last_name VARCHAR(100),
    email VARCHAR(150),
    phone VARCHAR(50),
    registration_date DATE,
    city VARCHAR(100),
    state VARCHAR(100),
    country VARCHAR(100),
    age_group VARCHAR(20),
    loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- ===============================
-- Products
-- ===============================
CREATE TABLE IF NOT EXISTS staging.products (
    product_id VARCHAR(20) PRIMARY KEY,
    product_name VARCHAR(150),
    category VARCHAR(100),
    sub_category VARCHAR(100),
    price NUMERIC(10,2),
    cost NUMERIC(10,2),
    brand VARCHAR(100),
    stock_quantity INT,
    supplier_id VARCHAR(20),
    loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- ===============================
-- Transactions (FIXED)
-- ===============================
CREATE TABLE IF NOT EXISTS staging.transactions (
    transaction_id VARCHAR(20) PRIMARY KEY,
    customer_id VARCHAR(20),
    transaction_date DATE,
    transaction_time TIME,
    payment_method VARCHAR(50),
    shipping_address TEXT,
    total_amount NUMERIC(10,2),
    loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- ===============================
-- Transaction Items (FIXED)
-- ===============================
CREATE TABLE IF NOT EXISTS staging.transaction_items (
    item_id VARCHAR(30) PRIMARY KEY,
    transaction_id VARCHAR(20),
    product_id VARCHAR(20),
    quantity INT,
    unit_price NUMERIC(10,2),
    discount_percentage INT,
    line_total NUMERIC(10,2),
    loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
