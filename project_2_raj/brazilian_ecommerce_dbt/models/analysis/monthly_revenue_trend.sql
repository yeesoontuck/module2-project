SELECT
  d.year,
  d.month,
  ROUND(SUM(f.total_item_value), 2) AS total_revenue
FROM {{ ref('fact_orders') }} f
JOIN {{ ref('dim_date') }} d ON f.order_date = d.date_id
GROUP BY d.year, d.month
ORDER BY d.year, d.month
