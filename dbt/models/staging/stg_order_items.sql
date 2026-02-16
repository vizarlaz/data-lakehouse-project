WITH source AS (
    SELECT * FROM {{ source('raw', 'order_items') }}
), 

cleaned AS (
    SELECT
        order_item_id,
        order_id,
        product_id,
        quantity,
        unit_price,
        discount_percent,
        quantity * unit_price AS gross_amount,
        quantity * unit_price * (1 - discount_percent/100) AS net_amount
    FROM source
    WHERE quantity > 0
)

SELECT * FROM cleaned