-- models/fact_orders.sql

WITH dim_date AS (
    SELECT
        date_id,
        year,
        month,
        day,
        quarter,
        weekday,
        date
    FROM {{ source('dim_date', 'dim_date') }}
),
olist_orders AS (
    SELECT
        order_id,
        customer_id,
        order_status,
        order_purchase_timestamp
    FROM {{ source('brazilian_ecommerce', 'orders') }}
)
-- Create the fact_orders table with the join to dim_date
SELECT
    o.order_id,
    o.customer_id,
    o.order_status,
    o.order_purchase_timestamp,
    EXTRACT(YEAR FROM o.order_purchase_timestamp) AS order_year,
    EXTRACT(MONTH FROM o.order_purchase_timestamp) AS order_month,
    EXTRACT(DAY FROM o.order_purchase_timestamp) AS order_day,
    d.date_id -- Reference to dim_date
FROM
    olist_orders o
JOIN
    dim_date d
ON
    DATE(o.order_purchase_timestamp) = d.date;