{{ config(materialized='view') }}

SELECT * 
FROM { source('brazilian_ecommerce', 'products') }
