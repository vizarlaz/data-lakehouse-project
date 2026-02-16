SELECT
    DATE(order_date) AS date,
    COUNT(DISTINCT order_id) AS total_orders,
    COUNT(DISTINCT customer_id) AS unique_customers,
    SUM(total_amount) AS revenue,
    SUM(total_amount) AS avg_order_value,
    SUM(items_count) AS items_sold
FROM {{ ref('fct_orders')}}
WHERE is_delivered = TRUE
GROUP BY DATE(order_date)
ORDER BY date DESC