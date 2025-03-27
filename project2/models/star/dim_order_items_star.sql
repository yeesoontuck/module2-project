{{ config(materialized='table') }}

WITH category_embed AS (
    SELECT
        `oi`.`order_id`,
        `oi`.`order_item_id`,
        `oi`.`product_id`,
        `t`.`product_category_name_english`
    FROM {{ source('brazil_e_commerce', 'order_items') }} AS oi
    JOIN {{ source('brazil_e_commerce', 'products') }} AS p
        ON oi.product_id = p.product_id
    LEFT JOIN {{ source('brazil_e_commerce', 'product_category_name_translation') }} AS t
        ON p.product_category_name = t.product_category_name
)
SELECT *
FROM category_embed