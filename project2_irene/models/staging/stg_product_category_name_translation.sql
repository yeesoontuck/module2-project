{{ config(materialized='view') }}

SELECT * 
FROM {{ source('brazilian_ecommerce', 'product_category_name_translation') }}
