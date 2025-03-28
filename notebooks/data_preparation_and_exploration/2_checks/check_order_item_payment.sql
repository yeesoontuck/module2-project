SELECT 
    o.order_id AS order_in_orders,
    o.order_status,
    o.order_purchase_timestamp,
    o.order_delivered_customer_date,
    oi.order_id AS order_in_items,
    oi.order_item_id AS itemid,
    oi.price,

    op.order_id AS order_in_payments,
    CONCAT(op.order_id, op.payment_sequential) AS paymentid,
    CASE 
        WHEN oi.order_id IS NULL AND op.order_id IS NOT NULL THEN op.payment_value
        ELSE NULL 
    END AS payment_without_items

FROM `brazilian_ecommerce.orders` o
FULL OUTER JOIN brazilian_ecommerce.order_items oi ON o.order_id = oi.order_id
FULL OUTER JOIN `brazilian_ecommerce.order_payments` op ON o.order_id = op.order_id
FULL OUTER JOIN `brazilian_ecommerce.order_items` oi2 ON op.order_id = oi2.order_id  -- Ensure full match between payments and items
WHERE o.order_id IS NULL OR oi.order_id IS NULL OR op.order_id IS NULL