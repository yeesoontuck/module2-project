SELECT 
  `order_id`, SUM(`price` + `freight_value`) as `total_order_value`
FROM {{ source('brazil_e_commerce', 'order_items') }}
GROUP BY `order_id`