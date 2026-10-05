SELECT
    session_id,
    MIN(event_ts)                                                AS session_start,
    CAST(MIN(event_ts) AS DATE)                                  AS session_date,
    MAX(customer_id)                                             AS customer_id,
    MAX(device)                                                  AS device,
    COUNT(*)                                                     AS events,
    MAX(CASE WHEN event_type = 'product_view' THEN 1 ELSE 0 END) AS reached_product_view,
    MAX(CASE WHEN event_type = 'add_to_cart'  THEN 1 ELSE 0 END) AS reached_add_to_cart,
    MAX(CASE WHEN event_type = 'checkout'     THEN 1 ELSE 0 END) AS reached_checkout,
    MAX(CASE WHEN event_type = 'purchase'     THEN 1 ELSE 0 END) AS reached_purchase
FROM {{ source('silver', 'web_events') }}
GROUP BY session_id