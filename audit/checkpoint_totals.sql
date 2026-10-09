-- ============================================================================
--  AtliQ Commerce  |  Checkpoint verification (section 7.7)
--
--  Run these against atliq.silver after a Silver build to confirm the cleaning
--  layer produced the expected figures. Each query carries its expected value
--  as a comment. Measured on the seed period (order_date <= 2026-08-31).
--
--  The strongest single check is the last pair: payments total and gross
--  revenue must agree exactly. That identity only holds once retried payments,
--  double-submitted line items and QA test accounts are each handled right.
-- ============================================================================

-- ---------------------------------------------------------------------------
-- Spot checks: did the tables land, and did the lookup maps conform?
-- ---------------------------------------------------------------------------

SHOW TABLES IN atliq.silver;

-- Expect only the canonical spellings. A stray 'bangalore' next to 'Bengaluru'
-- means a variant escaped CITY_MAP in 02_silver.
SELECT DISTINCT status FROM atliq.silver.orders;
SELECT DISTINCT city   FROM atliq.silver.customers;

-- Every cleaning rule and how many rows it touched.
SELECT * FROM atliq.silver.dq_log;

-- ---------------------------------------------------------------------------
-- Section 7.7 checkpoint totals
-- ---------------------------------------------------------------------------
-- Customers: expect 1,000 and exactly 10 cities
SELECT COUNT(*) AS customers, COUNT(DISTINCT city) AS cities
FROM atliq.silver.customers
WHERE NOT is_test;

-- Products: expect 60 and exactly 6 categories
SELECT COUNT(*) AS products, COUNT(DISTINCT category) AS categories
FROM atliq.silver.products;

-- Orders: expect 9,961 total
SELECT COUNT(*) AS orders
FROM atliq.silver.orders o
JOIN atliq.silver.customers c ON o.customer_id = c.customer_id
WHERE NOT c.is_test AND o.order_date <= '2026-08-31';

-- Status split: Placed 47, Shipped 102, Delivered 6,838, Cancelled 1,778, Returned 1,196
SELECT o.status, COUNT(*) AS n
FROM atliq.silver.orders o
JOIN atliq.silver.customers c ON o.customer_id = c.customer_id
WHERE NOT c.is_test AND o.order_date <= '2026-08-31'
GROUP BY o.status ORDER BY n DESC;

-- Marketing: expect 3,037 rows, spend 8,920,850.16, clicks 3,274,996
SELECT COUNT(*) AS rows, SUM(spend_amount) AS spend, SUM(clicks) AS clicks
FROM atliq.silver.marketing_spend;

-- Supplier current costs: expect 60 rows summing to 56,871.53
SELECT COUNT(*) AS rows, SUM(supplier_cost) AS total
FROM (
  SELECT product_id, supplier_cost,
         ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY effective_date DESC) AS rn
  FROM atliq.silver.supplier_price_list
) WHERE rn = 1;

-- Clickstream: expect 426 / 93,436 / 23,193
SELECT (SELECT COUNT(*) FROM atliq.silver.web_events_quarantine) AS quarantined,
       (SELECT COUNT(*) FROM atliq.silver.web_events)            AS clean_events,
       (SELECT COUNT(DISTINCT session_id) FROM atliq.silver.web_events) AS sessions;


-- Payments: expect 8,183 rows totalling 32,424,661.00
SELECT COUNT(*) AS payments, SUM(amount) AS payments_total
FROM (
  SELECT order_id, amount,
         ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY paid_at, payment_id) AS rn
  FROM atliq.silver.payments
) WHERE rn = 1;

-- ---------------------------------------------------------------------------
-- Gold-side revenue checks (run after dbt build)
--   gross    excluding Cancelled : 32,424,661.00
--   returned                     :  4,574,944.00
--   net      gross minus returned: 27,849,717.00
-- ---------------------------------------------------------------------------

SELECT
    SUM(CASE WHEN status <> 'Cancelled' THEN gross_revenue ELSE 0 END) AS gross_revenue,
    SUM(CASE WHEN status  = 'Returned'  THEN gross_revenue ELSE 0 END) AS returned_revenue,
    SUM(CASE WHEN status <> 'Cancelled' THEN gross_revenue ELSE 0 END)
  - SUM(CASE WHEN status  = 'Returned'  THEN gross_revenue ELSE 0 END) AS net_revenue
FROM atliq.gold.fact_sales
WHERE order_date <= '2026-08-31';

-- Unfiltered totals, used for the idempotency proof (run twice, compare).
SELECT COUNT(*) AS fact_rows, SUM(gross_revenue) AS total_gross_revenue
FROM atliq.gold.fact_sales;
