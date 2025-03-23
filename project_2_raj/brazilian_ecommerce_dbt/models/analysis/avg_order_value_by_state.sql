SELECT
  c.customer_state,
  ROUND(AVG(f.total_item_value), 2) AS avg_order_value
FROM {{ ref('fact_orders') }} f
JOIN {{ ref('dim_customer') }} c ON f.customer_id = c.customer_id
GROUP BY c.customer_state
ORDER BY avg_order_value DESC
