SELECT 
  `order_id`, ROUND(SUM(`price` + `freight_value`), 2) as `total_order_value`
FROM {{ source('brazil_e_commerce', 'order_items') }}
GROUP BY `order_id`