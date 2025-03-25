SELECT DISTINCT *
FROM {{ source('brazilian_ecommerce', 'customers') }}