-- Detect Abnormally High Transaction Amounts:
SELECT *
FROM transactions
WHERE amount > 10000; 

-- Detect High Frequency of Transactions in a Short Time Frame:
SELECT customer_id, COUNT(*) as transaction_count
FROM transactions
WHERE date_time BETWEEN '2023-12-01 10:55:00' AND '2023-12-01 11:05:00'
GROUP BY customer_id
HAVING transaction_count > 5;

-- Detect Transactions from Unusual Locations:
SELECT t.transaction_id,t.amount,t.date_time,t.location, c.location
FROM transactions t
JOIN customers c ON t.customer_id = c.customer_id
WHERE t.location <> c.location;

-- Detect Transactions with Customers on a Blacklist:
SELECT t.*
FROM transactions t
JOIN cust_external_data b ON t.customer_id = b.customer_id;

-- Detect transaction that contains suspecious items
SELECT t.*
FROM transactions that
JOIN blacklist b ON t.merchant_details = b.blacklist_info