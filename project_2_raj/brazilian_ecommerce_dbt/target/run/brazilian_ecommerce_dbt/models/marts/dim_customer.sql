

  create or replace view `ambient-scope-454609-d5`.`brazilian_ecommerce`.`dim_customer`
  OPTIONS()
  as SELECT DISTINCT
  customer_id,
  customer_unique_id,
  customer_city,
  customer_state
FROM `ambient-scope-454609-d5`.`brazilian_ecommerce`.`stg_customers`;

