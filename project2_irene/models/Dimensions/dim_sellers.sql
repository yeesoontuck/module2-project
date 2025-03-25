SELECT DISTINCT *
FROM {{ source('brazilian_ecommerce', 'sellers') }}