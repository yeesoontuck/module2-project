SELECT 
    `o`.`order_id`,
    `c`.`customer_city`,
    `o`.`customer_id`,
    `o`.`order_purchase_timestamp`,
    `o`.`order_delivered_customer_date`,
    DATE_DIFF(`o`.`order_delivered_customer_date`, `o`.`order_purchase_timestamp`, DAY) AS days_diff
FROM {{ source('brazil_e_commerce', 'orders') }} AS o
INNER JOIN {{ source('brazil_e_commerce', 'customers') }} AS c
ON `o`.`customer_id` = `c`.`customer_id`
where order_status = 'delivered'
and order_delivered_customer_date IS NOT NULL