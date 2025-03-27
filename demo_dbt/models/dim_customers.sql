{{ config(materialized='view') }}

SELECT
    customer_id,
    customer_unique_id,
    customer_city,
    customer_state
FROM `composer_test`.`olist_customers_raw`
