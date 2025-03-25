WITH products_cte AS (
    SELECT
        p.product_id,
        p.product_category_name,
        t.product_category_name_english,
        p.product_name_lenght as product_name_length,
        p.product_description_lenght as product_description_length,
        p.product_photos_qty,
        p.product_weight_g,
        p.product_length_cm,
        p.product_height_cm,
        p.product_width_cm
    FROM {{ source('brazil_e_commerce', 'products') }} AS p
    LEFT JOIN {{ source('brazil_e_commerce', 'product_category_name_translation') }} AS t
        ON p.product_category_name = t.product_category_name
)
SELECT
    *
FROM products_cte
