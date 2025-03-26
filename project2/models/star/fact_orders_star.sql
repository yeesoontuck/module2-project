/*
    Using an INNER JOIN here because some orders do not have order_items rows
    Found 775 orders with zero items
    Using CTE to include the total order value, by adding the price and freight_value from order_items table
*/

{{ config(materialized='table') }}

WITH total_price AS (
    SELECT 
        `o`.`order_id`, 
        `o`.`customer_id`,
        `o`.`order_status`,
        `o`.`order_purchase_timestamp`,
        `o`.`order_delivered_carrier_date`,
        `o`.`order_delivered_customer_date`,
        `o`.`order_estimated_delivery_date`,
        ROUND(SUM(`price` + `freight_value`), 2) as `total_order_value`
    FROM {{ source('brazil_e_commerce', 'order_items') }} AS oi
    INNER JOIN {{ source('brazil_e_commerce', 'orders') }} AS o
    ON oi.`order_id` = o.`order_id`
    GROUP BY `order_id`, `o`.`customer_id`, `o`.`order_status`, `o`.`order_purchase_timestamp`, `o`.`order_delivered_carrier_date`, `o`.`order_delivered_customer_date`, `o`.`order_estimated_delivery_date`
)
SELECT
    *
FROM total_price
