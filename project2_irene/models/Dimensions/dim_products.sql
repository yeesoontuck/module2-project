-- models/dim_products.sql
SELECT
    product_id,
    product_name,
    category,
    price
FROM {{ source('brazilian_ecommerce', 'products') }}
