SELECT
  order_id,
  customer_id,
  order_status,
  order_purchase_timestamp,
  DATE(order_purchase_timestamp) AS order_date
FROM {{ source('ecommerce_brazil', 'orders_dataset') }}
