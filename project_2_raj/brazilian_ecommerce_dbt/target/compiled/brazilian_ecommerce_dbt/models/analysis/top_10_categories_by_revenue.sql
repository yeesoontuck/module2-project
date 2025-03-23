SELECT
  p.product_category_name,
  ROUND(SUM(f.total_item_value), 2) AS revenue
FROM `ambient-scope-454609-d5`.`brazilian_ecommerce`.`fact_orders` f
JOIN `ambient-scope-454609-d5`.`brazilian_ecommerce`.`dim_product` p ON f.product_id = p.product_id
GROUP BY p.product_category_name
ORDER BY revenue DESC
LIMIT 10