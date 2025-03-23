SELECT
  order_id,
  customer_id,
  order_status,
  order_purchase_timestamp,
  DATE(order_purchase_timestamp) AS order_date
FROM `ambient-scope-454609-d5`.`brazilian_ecommerce`.`orders_dataset`