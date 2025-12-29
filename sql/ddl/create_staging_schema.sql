CREATE SCHEMA IF NOT EXISTS staging;

CREATE TABLE IF NOT EXISTS staging.customers (
    customer_id VARCHAR(20) PRIMARY KEY,
    first_name VARCHAR(100),
    last_name VARCHAR(100),
    email VARCHAR(150),
    gender VARCHAR(10),
    date_of_birth DATE,
    city VARCHAR(100),
    country VARCHAR(100),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

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

CREATE TABLE IF NOT EXISTS staging.transactions (
    transaction_id VARCHAR(20) PRIMARY KEY,
    customer_id VARCHAR(20),
    transaction_date TIMESTAMP,
    payment_method VARCHAR(50),
    total_amount NUMERIC(10,2)
);

CREATE TABLE IF NOT EXISTS staging.transaction_items (
    transaction_item_id VARCHAR(30) PRIMARY KEY,
    transaction_id VARCHAR(20),
    product_id VARCHAR(20),
    quantity INT,
    unit_price NUMERIC(10,2),
    total_price NUMERIC(10,2)
);
