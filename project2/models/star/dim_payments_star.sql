SELECT
    order_id,
    payment_sequential,
    payment_type,
    payment_installments,
    payment_value
FROM {{ source('brazil_e_commerce', 'order_payments') }}
