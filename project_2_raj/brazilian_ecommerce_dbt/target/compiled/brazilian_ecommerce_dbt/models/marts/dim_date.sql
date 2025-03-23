SELECT DISTINCT
  DATE(order_purchase_timestamp) AS date_id,
  EXTRACT(YEAR FROM order_purchase_timestamp) AS year,
  EXTRACT(MONTH FROM order_purchase_timestamp) AS month,
  EXTRACT(DAY FROM order_purchase_timestamp) AS day
FROM `ambient-scope-454609-d5`.`brazilian_ecommerce`.`orders_dataset`