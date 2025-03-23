-- Payments must match the total price + freight value of all order items in an order
-- Therefore return records where total payment value minus total order value is not zero
SELECT 
  `p`.`order_id`,
  (ROUND(SUM(`p`.`payment_value`), 2) - ROUND(`otv`.`total_order_value`, 2)) AS `payment_difference`
FROM  {{ source('brazil_e_commerce', 'order_payments') }} `p`
INNER JOIN  {{ source('brazil_e_commerce', 'dim_order_total_value') }} AS `otv`
ON `p`.`order_id` = `otv`.`order_id`
GROUP BY `p`.`order_id`, `otv`.`total_order_value`
HAVING `payment_difference` <> 0