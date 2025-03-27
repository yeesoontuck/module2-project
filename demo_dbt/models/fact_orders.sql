/*
    Using an INNER JOIN here because some orders do not have order_items rows
    Found 775 orders with zero items
    Using CTE to include the total order value, by adding the price and freight_value from order_items table
*/
{{ config(materialized='table') }}

WITH order_items AS (
    SELECT 
        `order_id`,
        (`price` + `freight_value`) AS `item_total_price`
    FROM `composer_test`.`olist_order_items_raw`
),

fact_orders AS (
    SELECT 
        `o`.`order_id`, 
        `o`.`customer_id`,
        `o`.`order_status`,
        `o`.`order_purchase_timestamp`,
        `o`.`order_delivered_carrier_date`,
        `o`.`order_delivered_customer_date`,
        `o`.`order_estimated_delivery_date`,
        ROUND(SUM(`oi`.`item_total_price`), 2) as `total_order_value`
    FROM order_items AS oi
    INNER JOIN `composer_test`.`olist_orders_raw` AS o
    ON oi.`order_id` = o.`order_id`
    GROUP BY `order_id`, `o`.`customer_id`, `o`.`order_status`, `o`.`order_purchase_timestamp`, `o`.`order_delivered_carrier_date`, `o`.`order_delivered_customer_date`, `o`.`order_estimated_delivery_date`
)

SELECT
    *
FROM fact_orders
