WITH source AS (
    SELECT * FROM {{source('raw', 'products') }}
),
cleaned AS(
    SELECT
        product_id,
        product_name,
        category,
        brand,
        price,
        cost,
        ROUND((price - cost) / cost * 100, 2) AS margin_percent,
        stock_quantity,
        CASE
            WHEN stock_quantity = 0 THEN 'out_of_stock'
            WHEN stock_quantity < 10 THEN 'low_stock'
            ELSE 'in_stock'
        END AS stock_status
    FROM source
    WHERE price > 0
)

SELECT * FROM cleaned