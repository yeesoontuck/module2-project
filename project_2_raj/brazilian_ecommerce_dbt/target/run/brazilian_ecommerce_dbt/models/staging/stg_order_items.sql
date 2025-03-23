

  create or replace view `ambient-scope-454609-d5`.`brazilian_ecommerce`.`stg_order_items`
  OPTIONS()
  as SELECT
  order_id,
  product_id,
  seller_id,
  price,
  freight_value
FROM `ambient-scope-454609-d5`.`brazilian_ecommerce`.`order_items_dataset`;

