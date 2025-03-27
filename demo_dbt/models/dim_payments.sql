{{ config(materialized='view') }}

SELECT
    order_id,
    payment_sequential,
    payment_type,
    payment_installments,
    payment_value
FROM `composer_test`.`olist_order_payments_raw`
