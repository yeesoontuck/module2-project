SELECT
  o.order_id,
  o.customer_id,
  oi.product_id,
  oi.seller_id,
  o.order_date,
  oi.price + oi.freight_value AS total_item_value
FROM `ambient-scope-454609-d5`.`brazilian_ecommerce`.`stg_orders` o
JOIN `ambient-scope-454609-d5`.`brazilian_ecommerce`.`stg_order_items` oi ON o.order_id = oi.order_id