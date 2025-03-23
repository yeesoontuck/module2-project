

  create or replace view `ambient-scope-454609-d5`.`brazilian_ecommerce`.`dim_product`
  OPTIONS()
  as SELECT DISTINCT
  product_id,
  product_category_name,
  product_name_lenght,
  product_description_lenght,
  product_weight_g,
  product_length_cm,
  product_height_cm,
  product_width_cm
FROM `ambient-scope-454609-d5`.`brazilian_ecommerce`.`products_dataset`;

