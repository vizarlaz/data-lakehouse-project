WITH orders AS (
    SELECT * FROM {{ ref('stg_orders') }}
),

customers AS (
    SELECT * FROM {{ ref('stg_customers')}}
),

order_items AS (
    SELECT
        order_id,
        COUNT(*) AS items_count,
        SUM(net_amount) AS items_total
    FROM {{ ref('stg_order_items')}}
    GROUP BY order_id
)

SELECT
    o.order_id,
    o.customer_id,
    c.email,
    c.first_name || ' ' || c.last_name AS customer_name,
    c.country,
    o.order_date,
    o.year_month,
    o.status,
    o.payment_method,
    o.total_amount,
    oi.items_count,
    oi.items_total,
    CASE WHEN o.status = 'delivered' THEN TRUE ELSE FALSE END AS is_delivered
FROM orders o
LEFT JOIN customers c ON o.customer_id = c.customer_id
LEFT JOIN order_items oi ON o.order_id = oi.order_id