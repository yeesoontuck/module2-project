{{ config(materialized='view') }}

WITH products AS (
    SELECT `product_id`, `product_category_name`
    FROM `composer_test`.`olist_products_raw`
),

translations AS (
    SELECT 
        product_category_name,
        product_category_name_english
    FROM `composer_test.olist_order_product_category_name_translation_raw`
),

category_embed AS (
    SELECT
        `oi`.`order_id`,
        `oi`.`order_item_id`,
        `oi`.`product_id`,
        `t`.`product_category_name_english`
    FROM `composer_test`.`olist_order_items_raw` AS oi
    JOIN products AS p
        ON oi.product_id = p.product_id
    LEFT JOIN translations AS t
        ON p.product_category_name = t.product_category_name
)

SELECT *
FROM category_embed