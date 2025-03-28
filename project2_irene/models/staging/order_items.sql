{{ config(materialized='view') }}

SELECT * 
FROM { source('brazilian_ecommerce', 'order_items') }
