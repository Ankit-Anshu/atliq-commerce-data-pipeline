SELECT o.order_id, o.customer_id, o.order_date, o.status, o.order_amount
FROM {{ source('silver', 'orders') }} o
JOIN {{ ref('stg_customers') }} c ON o.customer_id = c.customer_id
WHERE NOT c.is_test