-- Data Cleaning

-- 1. Remove Duplicates
-- 2. Standardize the data
-- 3. Null values or blank values
-- 4. Remove Any Columns

--
#Table1: Events

-- A new table is created and the data cleaning process is performed there.

CREATE TABLE events_staging
LIKE events;

SELECT *
FROM events_staging;

INSERT events_staging
SELECT *
FROM events;

--
#Removing Duplicates:
SELECT event_id, COUNT(*)
FROM events_staging
GROUP BY event_id
HAVING COUNT(*) > 1;

SELECT user_id, product_id, event_type, event_timestamp, session_id, COUNT(*)
FROM events_staging
GROUP BY user_id, product_id, event_type, event_timestamp, session_id
HAVING COUNT(*) > 1;

WITH cte AS
(
SELECT *,
ROW_NUMBER() OVER(PARTITION BY user_id, product_id, event_type, event_timestamp, session_id) AS row_num
FROM events_staging
)
SELECT *
FROM cte
WHERE row_num > 1;
#There is no duplicate

--
#Standardizing the Data:
SELECT DISTINCT event_id
FROM events_staging;

SELECT COUNT(*)
FROM events_staging
WHERE event_id IS NULL;

--

SELECT DISTINCT user_id
FROM events_staging;

SELECT COUNT(*)
FROM events_staging
WHERE user_id IS NULL;

#Orphan user control: A orphan record is a record whose foreign key does not have a corresponding reference in the parent table.
#The question is: How many event records cannot find a match in the users table?

#Calculate the number of orphans:
SELECT COUNT(*)
FROM events_staging as e
LEFT JOIN users u ON e.user_id = u.user_id
WHERE u.user_id IS NULL;

#Find event records that have no corresponding entry in the users table and save them to a new table called orphan_events:
CREATE TABLE orphan_events AS
SELECT e.*
FROM events_staging as e
LEFT JOIN users u ON e.user_id = u.user_id
WHERE u.user_id IS NULL;

#Delete Orphan Records from Main Table:
DELETE e
FROM events_staging as e
LEFT JOIN users u ON e.user_id = u.user_id
WHERE u.user_id IS NULL;

--

SELECT DISTINCT product_id
FROM events_staging;

SELECT COUNT(*)
FROM events_staging
WHERE product_id IS NULL;

SELECT COUNT(*)
FROM events_staging as e
LEFT JOIN products as p ON e.product_id = p.product_id
WHERE p.product_id IS NULL;

--

SELECT DISTINCT event_type
FROM events_staging;

SELECT COUNT(*)
FROM events_staging
WHERE event_type IS NULL;

SELECT event_type, COUNT(*)
FROM events_staging
GROUP BY event_type
ORDER BY COUNT(*) desc;

--

SELECT DISTINCT event_timestamp
FROM events_staging;

SELECT COUNT(*)
FROM events_staging
WHERE event_timestamp IS NULL;

SELECT MIN(event_timestamp), MAX(event_timestamp)
FROM events_staging;

SELECT *
FROM events_staging
WHERE event_timestamp > NOW();
--

SELECT DISTINCT session_id
FROM events_staging;

SELECT COUNT(*)
FROM events_staging
WHERE session_id IS NULL;

#Logic check within the same session:
SELECT session_id
FROM events_staging
GROUP BY session_id
HAVING SUM(event_type = 'page_view') = 0
	AND SUM(event_type = 'purchase') > 0;

SELECT COUNT(*) 
FROM (
    SELECT session_id
    FROM events_staging
    GROUP BY session_id
    HAVING SUM(event_type = 'page_view') = 0
       AND SUM(event_type = 'purchase') > 0
) t;

#Identified ~10% of sessions with purchase but no page_view, flagged them to ensure accurate funnel and conversion analysis.

ALTER TABLE events_staging 
ADD COLUMN funnel_issue_flag TINYINT DEFAULT 0;

UPDATE events_staging as e
JOIN (
    SELECT session_id
    FROM events_staging
    GROUP BY session_id
    HAVING SUM(event_type = 'page_view') = 0
       AND SUM(event_type = 'purchase') > 0
) as problematic_sessions
ON e.session_id = problematic_sessions.session_id
SET e.funnel_issue_flag = 1;

#Filtering for analysis:
SELECT *
FROM events_staging
WHERE funnel_issue_flag = 0;

--

SELECT DISTINCT device_type
FROM events_staging;

SELECT COUNT(*)
FROM events_staging
WHERE device_type IS NULL;

--

SELECT DISTINCT traffic_source
FROM events_staging;

SELECT COUNT(*)
FROM events_staging
WHERE traffic_source IS NULL;

--

SELECT DISTINCT session_duration_seconds
FROM events_staging;

SELECT COUNT(*)
FROM events_staging
WHERE session_duration_seconds IS NULL;

SELECT *
FROM events_staging
WHERE session_duration_seconds < 0;

ALTER TABLE events_staging
MODIFY COLUMN event_type VARCHAR(50),
MODIFY COLUMN event_timestamp DATETIME,
MODIFY COLUMN device_type VARCHAR(50),
MODIFY COLUMN traffic_source VARCHAR(50),
MODIFY COLUMN session_duration_seconds INT;
--

SELECT *
FROM events_staging;

#Clean data:
CREATE VIEW events_clean AS
SELECT *
FROM events_staging
WHERE funnel_issue_flag = 0;

--
#Table2: Orders

SELECT *
FROM orders;

CREATE TABLE orders_staging
LIKE orders;

INSERT INTO orders_staging
SELECT *
FROM orders;

SELECT *
FROM orders_staging;

--
#Removing Duplicates:

#Orderid based:
SELECT order_id, COUNT(*)
FROM orders_staging
GROUP BY order_id
HAVING COUNT(*) > 1;

#Business Key based:
SELECT user_id, product_id, order_date, COUNT(*)
FROM orders_staging
GROUP BY user_id, product_id, order_date
HAVING COUNT(*) > 1;

-- 
#Standardizing the Data:
SELECT COUNT(*) FROM orders_staging WHERE order_id IS NULL;

SELECT COUNT(*) FROM orders_staging WHERE user_id IS NULL;

SELECT COUNT(*) FROM orders_staging WHERE order_date IS NULL;

SELECT COUNT(*) FROM orders_staging WHERE product_id IS NULL;

SELECT DISTINCT quantity from orders_staging;
SELECT COUNT(*) FROM orders_staging WHERE quantity IS NULL;

SELECT COUNT(*) FROM orders_staging WHERE unit_price IS NULL;

SELECT COUNT(*) FROM orders_staging WHERE discount_amount IS NULL;

SELECT DISTINCT payment_method FROM orders_staging;
SELECT COUNT(*) FROM orders_staging WHERE payment_method IS NULL;

SELECT DISTINCT order_status FROM orders_staging;
SELECT COUNT(*) FROM orders_staging WHERE order_status IS NULL;

SELECT DISTINCT refund_flag FROM orders_staging;
SELECT COUNT(*) FROM orders_staging WHERE refund_flag IS NULL;

SELECT COUNT(*) FROM orders_staging WHERE total_amount IS NULL;

ALTER TABLE orders_staging
MODIFY COLUMN unit_price DECIMAL(10,2),
MODIFY COLUMN discount_amount DECIMAL(10,2),
MODIFY COLUMN total_amount DECIMAL(10,2),
MODIFY COLUMN order_date DATETIME;

--
#Orphan controls:

#User Orphan:
SELECT COUNT(*) 
FROM orders_staging as o
LEFT JOIN users as u ON o.user_id = u.user_id
WHERE u.user_id IS NULL;

SELECT COUNT(*)
FROM orders_staging as o
LEFT JOIN products as p ON o.product_id = p.product_id
WHERE p.product_id IS NULL;

CREATE TABLE orphan_orders AS
SELECT o.*
FROM orders_staging as o
LEFT JOIN users as u ON o.user_id = u.user_id
WHERE u.user_id IS NULL;

DELETE o
FROM orders_staging as o
LEFT JOIN users u ON o.user_id = u.user_id
WHERE u.user_id IS NULL;

--
#Logic Controls:
SELECT *
FROM orders_staging
WHERE quantity <= 0;

SELECT *
FROM orders_staging
WHERE unit_price <= 0;

SELECT *
FROM orders_staging
WHERE discount_amount > (quantity * unit_price);

SELECT *
FROM orders_staging
WHERE total_amount != (quantity * unit_price - discount_amount);

SELECT *
FROM orders_staging
WHERE refund_flag = 1
AND order_status NOT IN ('cancelled','returned');

SELECT COUNT(*)
FROM orders_staging
WHERE refund_flag = 1
AND order_status IN ('completed','shipped');

UPDATE orders_staging
SET order_status = 'returned'
WHERE refund_flag = 1
AND order_status IN ('completed','shipped');

SELECT *
FROM orders_staging
WHERE order_date > NOW();

SELECT o.*
FROM orders_staging as o
JOIN users as u ON o.user_id = u.user_id
WHERE o.order_date < u.signup_date;

SELECT 
	o.user_id,
    o.order_date,
    u.signup_date,
    DATEDIFF(u.signup_date, o.order_date) AS diff_days
FROM orders_staging as o 
JOIN users as u ON o.user_id = u.user_id
WHERE o.order_date < u.signup_date;

UPDATE users as u
JOIN (
    SELECT user_id, MIN(order_date) AS first_order
    FROM orders_staging
    GROUP BY user_id
) t ON u.user_id = t.user_id
SET u.signup_date = t.first_order
WHERE u.signup_date > t.first_order;

SELECT COUNT(*)
FROM orders_staging as o
JOIN users as u ON o.user_id = u.user_id
WHERE o.order_date < u.signup_date;

SELECT * FROM orders_staging;

--
#Table 3: Products

SELECT * FROM products;

CREATE TABLE products_staging
LIKE products;

INSERT INTO products_staging
SELECT *
FROM products;

SELECT * FROM products_staging;

--

SELECT product_id, COUNT(*)
FROM products_staging
GROUP BY product_id
HAVING COUNT(*) > 1 ;

SELECT product_name, COUNT(*)
FROM products_staging
GROUP BY product_name
HAVING COUNT(*) > 1 ;

SELECT COUNT(*) FROM products_staging WHERE product_id IS NULL;

SELECT COUNT(*) FROM products_staging WHERE product_name IS NULL;

SELECT COUNT(*) FROM products_staging WHERE category IS NULL;

SELECT COUNT(*) FROM products_staging WHERE sub_category IS NULL;

SELECT COUNT(*) FROM products_staging WHERE brand IS NULL;

SELECT COUNT(*) FROM products_staging WHERE price IS NULL;

SELECT COUNT(*) FROM products_staging WHERE discount_price IS NULL;

SELECT COUNT(*) FROM products_staging WHERE cost IS NULL;

SELECT COUNT(*) FROM products_staging WHERE launch_date IS NULL;

SELECT COUNT(*) FROM products_staging WHERE seller_id IS NULL;

SELECT COUNT(*) FROM products_staging WHERE stock_quantity IS NULL;

ALTER TABLE products_staging
MODIFY COLUMN price DECIMAL(10,2),
MODIFY COLUMN discount_price DECIMAL(10,2),
MODIFY COLUMN cost DECIMAL(10,2),
MODIFY COLUMN launch_date DATETIME;

--
SELECT DISTINCT category FROM products_staging;

SELECT DISTINCT sub_category FROM products_staging;

SELECT DISTINCT brand FROM products_staging;

--

SELECT *
FROM products_staging
WHERE price <= 0;

SELECT *
FROM products_staging
WHERE cost <= 0;

SELECT *
FROM products_staging
WHERE cost > price;

SELECT *
FROM products_staging
WHERE stock_quantity < 0;

#Are the product_ids in the orders table actually present in products?
SELECT COUNT(*)
FROM orders_staging as o
LEFT JOIN products_staging as p
ON o.product_id = p.product_id
WHERE p.product_id IS NULL;

ALTER TABLE products_staging
ADD column margin DECIMAL(10,2);

UPDATE products_staging
SET margin = price - cost;


#TRUNCATE TABLE products_staging;
#INSERT INTO products_staging
#SELECT * FROM products;

--
#Table4: Users

SELECT * FROM users_staging;

CREATE TABLE users_staging LIKE users;

INSERT INTO users_staging
SELECT * FROM users;

--
SELECT user_id, COUNT(*)
FROM users_staging
GROUP BY user_id
HAVING COUNT(*) > 1;

SELECT user_id, COUNT(*)
FROM users_staging
GROUP BY user_id
HAVING COUNT(*) > 1;

SELECT COUNT(*) FROM users_staging WHERE user_id IS NULL;
SELECT COUNT(*) FROM users_staging WHERE signup_date IS NULL;
SELECT COUNT(*) FROM users_staging WHERE country IS NULL;
SELECT COUNT(*) FROM users_staging WHERE device_type IS NULL;
SELECT COUNT(*) FROM users_staging WHERE traffic_source IS NULL;
SELECT COUNT(*) FROM users_staging WHERE is_new_customer IS NULL;
SELECT COUNT(*) FROM users_staging WHERE favorite_product_id IS NULL;
SELECT COUNT(*) FROM users_staging WHERE lifetime_orders IS NULL;
SELECT COUNT(*) FROM users_staging WHERE lifetime_value IS NULL;

ALTER TABLE users_staging
MODIFY COLUMN signup_date DATETIME;

--
SELECT *
FROM users_staging
WHERE signup_date > CURRENT_DATE;

SELECT *
FROM users_staging
WHERE is_new_customer NOT IN (0,1);

SELECT *
FROM users_staging
WHERE lifetime_orders < 0;

SELECT *
FROM users_staging
WHERE lifetime_value < 0;

SELECT DISTINCT country FROM users_staging;
SELECT DISTINCT device_type FROM users_staging;
SELECT DISTINCT traffic_source FROM users_staging;

#Does favorite_product_id actually exist in the products table?:
SELECT COUNT(*)
FROM users_staging as u
LEFT JOIN products_staging as p
ON u.favorite_product_id = p.product_id
WHERE p.product_id IS NULL;

--











SELECT * FROM users_staging;
