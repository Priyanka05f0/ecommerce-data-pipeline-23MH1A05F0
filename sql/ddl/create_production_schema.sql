CREATE SCHEMA IF NOT EXISTS production;

-- ============================
-- PRODUCTION TABLES (3NF)
-- ============================

CREATE TABLE IF NOT EXISTS production.customers (
    customer_id VARCHAR(20) PRIMARY KEY,
    first_name VARCHAR(100) NOT NULL,
    last_name VARCHAR(100) NOT NULL,
    email VARCHAR(150) NOT NULL UNIQUE,
    phone VARCHAR(50),
    registration_date DATE,
    city VARCHAR(100),
    state VARCHAR(100),
    country VARCHAR(100),
    age_group VARCHAR(20),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS production.products (
    product_id VARCHAR(20) PRIMARY KEY,
    product_name VARCHAR(150) NOT NULL,
    category VARCHAR(100),
    sub_category VARCHAR(100),
    price DECIMAL(10,2) CHECK (price >= 0),
    cost DECIMAL(10,2) CHECK (cost >= 0),
    brand VARCHAR(100),
    stock_quantity INTEGER CHECK (stock_quantity >= 0),
    supplier_id VARCHAR(20),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS production.transactions (
    transaction_id VARCHAR(20) PRIMARY KEY,
    customer_id VARCHAR(20) NOT NULL,
    transaction_date DATE,
    transaction_time TIME,
    payment_method VARCHAR(50),
    shipping_address TEXT,
    total_amount DECIMAL(12,2) CHECK (total_amount >= 0),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT fk_customer
        FOREIGN KEY (customer_id)
        REFERENCES production.customers(customer_id)
);

CREATE TABLE IF NOT EXISTS production.transaction_items (
    item_id VARCHAR(20) PRIMARY KEY,
    transaction_id VARCHAR(20) NOT NULL,
    product_id VARCHAR(20) NOT NULL,
    quantity INTEGER CHECK (quantity > 0),
    unit_price DECIMAL(10,2) CHECK (unit_price >= 0),
    discount_percentage INTEGER CHECK (discount_percentage BETWEEN 0 AND 100),
    line_total DECIMAL(12,2) CHECK (line_total >= 0),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT fk_transaction
        FOREIGN KEY (transaction_id)
        REFERENCES production.transactions(transaction_id),
    CONSTRAINT fk_product
        FOREIGN KEY (product_id)
        REFERENCES production.products(product_id)
);

-- ============================
-- INDEXES FOR PERFORMANCE
-- ============================

CREATE INDEX IF NOT EXISTS idx_transactions_date
ON production.transactions(transaction_date);

CREATE INDEX IF NOT EXISTS idx_transactions_customer
ON production.transactions(customer_id);

CREATE INDEX IF NOT EXISTS idx_items_transaction
ON production.transaction_items(transaction_id);

CREATE INDEX IF NOT EXISTS idx_items_product
ON production.transaction_items(product_id);
