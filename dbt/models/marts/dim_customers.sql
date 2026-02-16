WITH customers AS (
    SELECT * FROM {{ ref('stg_customers')}}
),

orders AS (
    SELECT
        customer_id,
        COUNT(*) AS total_orders,
        SUM(CASE WHEN is_delivered THEN total_amount ELSE 0 END) AS lifetime_value,
        MIN(order_date) AS first_order,
        MAX(order_date) AS last_order
    FROM {{ ref('fct_orders') }}
    GROUP BY customer_id
)

SELECT
    c.customer_id,
    c.email,
    c.first_name,
    c.last_name,
    c.country,
    c.city,
    COALESCE(o.total_orders, 0) AS total_orders,
    COALESCE(o.lifetime_value, 0) AS lifetime_value,
    o.first_order,
    o.last_order,
    CASE
        WHEN o.lifetime_value >= 1000 THEN 'VIP'
        WHEN o.lifetime_value >= 500 THEN 'Gold'
        ELSE 'Standard'
    END AS tier
FROM customers c
LEFT JOIN orders o ON c.customer_id = o.customer_id