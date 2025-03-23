SELECT DISTINCT
  seller_id,
  seller_city,
  seller_state
FROM {{ source('ecommerce_brazil', 'sellers_dataset') }}
