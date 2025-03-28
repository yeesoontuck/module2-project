WITH order_sums AS (
  -- Summing the total payment value at the order level
  SELECT 
      o.order_id,
      SUM(op.payment_value) AS total_payment_value
  FROM {{ source('brazilian_ecommerce', 'orders') }} o
  LEFT JOIN {{ source('brazilian_ecommerce', 'order_payments') }} op
  ON o.order_id = op.order_id
  GROUP BY o.order_id
),

order_item_values AS (
  -- Summing the total order value at the item level
  SELECT 
      oi.order_id,
      SUM(oi.price) AS total_order_value
  FROM {{ source('brazilian_ecommerce', 'order_items') }} oi
  GROUP BY oi.order_id
)

SELECT
  oi.order_item_id,
  oi.order_id,
  o.customer_id,
  o.order_status,
  o.order_purchase_timestamp,
  o.order_delivered_carrier_date,
  o.order_delivered_customer_date,
  o.order_estimated_delivery_date,
  oi.seller_id,
  oi.product_id,
  oi.price,
  oi.price / oiv.total_order_value AS proportion,
  oi.shipping_limit_date,
  oi.freight_value,
  os.total_payment_value,
  os.total_payment_value * (oi.price / oiv.total_order_value) AS prop_payment_value
FROM {{ source('brazilian_ecommerce', 'order_items') }} oi
JOIN {{ source('brazilian_ecommerce', 'orders') }} o
  ON oi.order_id = o.order_id
JOIN order_sums os
  ON oi.order_id = os.order_id
JOIN order_item_values oiv
  ON oi.order_id = oiv.order_id