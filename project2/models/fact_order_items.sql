SELECT
    order_id,
    order_item_id,
    product_id,
    price,
    freight_value
FROM {{ source('brazil_e_commerce', 'order_items') }}
