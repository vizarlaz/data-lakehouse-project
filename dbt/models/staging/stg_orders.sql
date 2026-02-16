WITH source AS (
    SELECT * FROM {{ source('raw', 'orders') }}
),

cleaned AS (
    SELECT 
        order_id,
        customer_id,
        order_date,
        total_amount,
        status,
        payment_method,
        EXTRACT(YEAR FROM order_date) AS order_year,
        EXTRACT(MONTH FROM order_date) AS order_month,
        TO_CHAR(order_date, 'YYYY-MM') AS year_month
    FROM source
    WHERE total_amount > 0
)

SELECT * FROM cleaned