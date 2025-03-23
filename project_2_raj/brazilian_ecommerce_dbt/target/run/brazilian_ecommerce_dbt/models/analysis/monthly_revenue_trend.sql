

  create or replace view `ambient-scope-454609-d5`.`brazilian_ecommerce`.`monthly_revenue_trend`
  OPTIONS()
  as SELECT
  d.year,
  d.month,
  ROUND(SUM(f.total_item_value), 2) AS total_revenue
FROM `ambient-scope-454609-d5`.`brazilian_ecommerce`.`fact_orders` f
JOIN `ambient-scope-454609-d5`.`brazilian_ecommerce`.`dim_date` d ON f.order_date = d.date_id
GROUP BY d.year, d.month
ORDER BY d.year, d.month;

