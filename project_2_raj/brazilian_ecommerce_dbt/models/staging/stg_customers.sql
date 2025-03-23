SELECT
  customer_id,
  customer_unique_id,
  customer_city,
  customer_state
FROM {{ source('ecommerce_brazil', 'customer_dataset') }}
