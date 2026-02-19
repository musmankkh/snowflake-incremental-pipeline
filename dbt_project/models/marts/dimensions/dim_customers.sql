{{
    config(
        materialized='table'
    )
}}

SELECT DISTINCT
    customer_id,
    country,
    payment_method,
    sales_channel,
    is_guest_customer,
    MIN(invoice_date) AS first_purchase_date,
    MAX(invoice_date) AS last_purchase_date,
    COUNT(DISTINCT invoice_no) AS total_orders
FROM {{ ref('processed_sales') }}
WHERE customer_id IS NOT NULL
GROUP BY
    customer_id,
    country,
    payment_method,
    sales_channel,
    is_guest_customer
