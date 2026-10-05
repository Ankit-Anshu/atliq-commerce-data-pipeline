SELECT customer_id, customer_name, email, city, signup_date, is_test
FROM {{ source('silver', 'customers') }}