-- ===============================
-- COMPLETENESS CHECKS
-- ===============================

-- Null checks
SELECT 'customers.email' AS check_name, COUNT(*) AS violations
FROM staging.customers
WHERE email IS NULL;

SELECT 'products.price' AS check_name, COUNT(*) AS violations
FROM staging.products
WHERE price IS NULL OR price <= 0;

-- ===============================
-- UNIQUENESS CHECKS
-- ===============================

-- Duplicate customer IDs
SELECT customer_id, COUNT(*) 
FROM staging.customers
GROUP BY customer_id
HAVING COUNT(*) > 1;

-- Duplicate emails
SELECT email, COUNT(*) 
FROM staging.customers
GROUP BY email
HAVING COUNT(*) > 1;

-- ===============================
-- REFERENTIAL INTEGRITY
-- ===============================

-- Orphan transactions
SELECT COUNT(*) AS orphan_transactions
FROM staging.transactions t
LEFT JOIN staging.customers c
ON t.customer_id = c.customer_id
WHERE c.customer_id IS NULL;

-- Orphan transaction items (transaction)
SELECT COUNT(*) AS orphan_items_txn
FROM staging.transaction_items ti
LEFT JOIN staging.transactions t
ON ti.transaction_id = t.transaction_id
WHERE t.transaction_id IS NULL;

-- Orphan transaction items (product)
SELECT COUNT(*) AS orphan_items_product
FROM staging.transaction_items ti
LEFT JOIN staging.products p
ON ti.product_id = p.product_id
WHERE p.product_id IS NULL;

-- ===============================
-- CONSISTENCY CHECKS
-- ===============================

-- Line total mismatch
SELECT COUNT(*) AS line_total_mismatch
FROM staging.transaction_items
WHERE line_total <> quantity * unit_price * (1 - discount_percentage/100);

-- Transaction total mismatch
SELECT COUNT(*) AS txn_total_mismatch
FROM staging.transactions t
JOIN staging.transaction_items ti
ON t.transaction_id = ti.transaction_id
GROUP BY t.transaction_id, t.total_amount
HAVING t.total_amount <> SUM(ti.line_total);
