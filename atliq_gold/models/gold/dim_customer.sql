SELECT c.customer_id, c.customer_name, c.city, c.signup_date,
       date_trunc('month', c.signup_date) AS signup_cohort,
       f.first_order_date
FROM {{ ref('stg_customers') }} c
LEFT JOIN (
    SELECT customer_id, MIN(order_date) AS first_order_date
    FROM {{ ref('stg_orders') }}
    GROUP BY customer_id
) f ON c.customer_id = f.customer_id
WHERE NOT c.is_test