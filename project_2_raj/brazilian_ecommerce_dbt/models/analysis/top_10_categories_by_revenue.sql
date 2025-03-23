SELECT
  p.product_category_name,
  ROUND(SUM(f.total_item_value), 2) AS revenue
FROM {{ ref('fact_orders') }} f
JOIN {{ ref('dim_product') }} p ON f.product_id = p.product_id
GROUP BY p.product_category_name
ORDER BY revenue DESC
LIMIT 10
