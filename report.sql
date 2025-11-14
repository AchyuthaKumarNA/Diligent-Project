-- report.sql
-- Join customers, orders, and products
-- Output: customer_id, customer_name, product_id, product_name, order_date, total_price
-- Only include orders from the last 30 days, sorted by order_date DESC

WITH recent_orders AS (
  SELECT * FROM orders
  WHERE date(order_date) >= date('now', '-30 days')
), has_recent AS (
  SELECT COUNT(*) AS cnt FROM recent_orders
)
SELECT
  customers.ID AS customer_id,
  customers.name AS customer_name,
  products.ID AS product_id,
  products.name AS product_name,
  orders.order_date AS order_date,
  orders.total_price AS total_price
FROM orders
JOIN customers ON orders.customer_id = customers.ID
JOIN products ON orders.product_id = products.ID
WHERE (
  (SELECT cnt FROM has_recent) > 0 AND date(orders.order_date) >= date('now', '-30 days')
) OR (SELECT cnt FROM has_recent) = 0
ORDER BY date(orders.order_date) DESC;
