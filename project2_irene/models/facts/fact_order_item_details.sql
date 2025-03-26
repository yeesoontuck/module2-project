WITH order_sums AS (
  SELECT 
      oi.order_id,
      SUM(oi.price) AS total_order_value,
      SUM(op.payment_value) AS total_payment_value
  FROM {{ source('brazilian_ecommerce', 'order_items') }} oi
  LEFT JOIN {{ source('brazilian_ecommerce', 'order_payments') }} op
  ON oi.order_id = op.order_id
  GROUP BY oi.order_id
)

SELECT
  oi.order_item_id,
  oi.order_id,
  oi.seller_id,
  oi.product_id,
  oi.price,
  oi.price / os.total_order_value AS proportion,
  os.total_payment_value,
  os.total_payment_value * (oi.price / os.total_order_value) AS prop_payment_value,


FROM {{ source('brazilian_ecommerce', 'order_items') }} oi
JOIN order_sums os
ON oi.order_id = os.order_id
