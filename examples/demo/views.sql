-- ivm side of the demo: a source, maintained views, an index and a sink.
-- Run with psql variables: -v upstream='<conninfo of db shop>' -v sinkconn='<conninfo of db sinkdb>'

CREATE SOURCE shop FROM POSTGRES CONNECTION :'upstream' PUBLICATION 'shop_pub';

-- joins + aggregates
CREATE MATERIALIZED VIEW revenue_by_region AS
SELECT r.name AS region, count(DISTINCT o.id) AS orders, sum(i.qty * i.unit_price) AS revenue
FROM orders o
JOIN customers c ON c.id = o.customer_id
JOIN regions r ON r.id = c.region_id
JOIN order_items i ON i.order_id = o.id
WHERE o.status <> 'cancelled'
GROUP BY r.name;

-- maintained top-k
CREATE MATERIALIZED VIEW top_products AS
SELECT p.id, p.name, p.category, sum(i.qty) AS units, sum(i.qty * i.unit_price) AS revenue
FROM order_items i JOIN products p ON p.id = i.product_id
GROUP BY p.id, p.name, p.category
ORDER BY revenue DESC
LIMIT 10;

-- a view over a view (shares its dataflow)
CREATE MATERIALIZED VIEW region_leader AS
SELECT region, revenue FROM revenue_by_region ORDER BY revenue DESC LIMIT 1;

-- temporal filter: rows enter and leave as the logical clock advances
CREATE MATERIALIZED VIEW recent_orders AS
SELECT o.id, c.name AS customer, o.status, o.created
FROM orders o JOIN customers c ON c.id = o.customer_id
WHERE o.created > now() - interval '2 minutes';

-- outer joins + aggregates, with an index
CREATE MATERIALIZED VIEW customer_ltv AS
SELECT c.id, c.name, c.tier, r.name AS region,
       count(DISTINCT o.id) AS orders,
       coalesce(sum(i.qty * i.unit_price), 0) AS lifetime_value,
       max(o.created) AS last_order
FROM customers c
JOIN regions r ON r.id = c.region_id
LEFT JOIN orders o ON o.customer_id = c.id AND o.status <> 'cancelled'
LEFT JOIN order_items i ON i.order_id = o.id
GROUP BY c.id, c.name, c.tier, r.name;
CREATE INDEX ON customer_ltv (region);

-- ordered-set aggregates
CREATE MATERIALIZED VIEW category_stats AS
SELECT p.category, count(*) AS lines,
       percentile_cont(0.5) WITHIN GROUP (ORDER BY i.qty * i.unit_price) AS median_line,
       mode() WITHIN GROUP (ORDER BY i.qty) AS typical_qty
FROM order_items i JOIN products p ON p.id = i.product_id
GROUP BY p.category;

-- window functions over a derived table
CREATE MATERIALIZED VIEW order_running_totals AS
SELECT o.customer_id, o.id AS order_id, o.created,
       sum(t.total) OVER (PARTITION BY o.customer_id ORDER BY o.created, o.id) AS running_total,
       row_number() OVER (PARTITION BY o.customer_id ORDER BY o.created DESC, o.id DESC) AS recency
FROM orders o
JOIN (SELECT order_id, sum(qty * unit_price) AS total FROM order_items GROUP BY order_id) t
  ON t.order_id = o.id;

-- simple filter and a fast-changing aggregate
CREATE MATERIALIZED VIEW low_stock AS
SELECT id, name, category, stock FROM products WHERE stock < 10;

CREATE MATERIALIZED VIEW order_status_counts AS
SELECT status, count(*) AS n FROM orders GROUP BY status;

-- SQL/JSON path, and a set-returning function under a temporal filter
CREATE MATERIALIZED VIEW expensive_interest AS
SELECT kind, count(*) AS events
FROM events
WHERE payload @? '$.items[*] ? (@.price > 200)'
GROUP BY kind;

CREATE MATERIALIZED VIEW recent_event_items AS
SELECT id, kind, jsonb_path_query(payload, '$.items[*].product') AS product
FROM events
WHERE at > now() - interval '5 minutes';

-- errors as data: every few minutes the load generator sets a product's
-- price to 0 for 20 seconds; while it is 0, reads of this view report
-- "division by zero", and they work again once the price is restored
CREATE MATERIALIZED VIEW units_per_100 AS
SELECT id, name, round(100 / price, 2) AS units FROM products;

-- a virtual (non-materialized) view
CREATE VIEW gold_customers AS SELECT * FROM customer_ltv WHERE tier = 'gold';

-- write one view back to PostgreSQL
CREATE SINK region_sink FROM revenue_by_region
  INTO POSTGRES CONNECTION :'sinkconn' TABLE 'revenue_by_region' KEY (region);
