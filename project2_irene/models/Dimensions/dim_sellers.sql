SELECT DISTINCT
    seller_id, 
    seller_zip_code_prefix, 
    seller_state, 
    seller_city
FROM {{ source('brazilian_ecommerce', 'sellers') }}