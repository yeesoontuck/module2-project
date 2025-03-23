SELECT
  o.order_id,
  o.customer_id,
  MIN(o.order_date) AS order_date,
  COUNT(DISTINCT oi.product_id) AS distinct_product_count,
  COUNT(oi.product_id) AS total_items,
  SUM(oi.price) AS total_price,
  SUM(oi.freight_value) AS total_freight,
  SUM(oi.price + oi.freight_value) AS total_order_value
FROM {{ ref('stg_orders') }} o
JOIN {{ ref('stg_order_items') }} oi
  ON o.order_id = oi.order_id
GROUP BY
  o.order_id,
  o.customer_id



