

  create or replace view `ambient-scope-454609-d5`.`brazilian_ecommerce`.`avg_order_value_by_state`
  OPTIONS()
  as SELECT
  c.customer_state,
  ROUND(AVG(f.total_item_value), 2) AS avg_order_value
FROM `ambient-scope-454609-d5`.`brazilian_ecommerce`.`fact_orders` f
JOIN `ambient-scope-454609-d5`.`brazilian_ecommerce`.`dim_customer` c ON f.customer_id = c.customer_id
GROUP BY c.customer_state
ORDER BY avg_order_value DESC;

