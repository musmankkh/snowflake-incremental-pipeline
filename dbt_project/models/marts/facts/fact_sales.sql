{{
    config(
        materialized='incremental',
        unique_key=['invoice_no','stock_code'],
        incremental_strategy='merge'
    )
}}

SELECT
    invoice_no,
    stock_code,
    customer_id,
    invoice_date::DATE AS date_key,

    quantity,
    unit_price,
    discount,
    shipping_cost,

    gross_amount,
    net_amount,
    total_amount,

    is_returned,
    processed_at

FROM {{ ref('processed_sales') }}

{% if is_incremental() %}
WHERE processed_at > (SELECT MAX(processed_at) FROM {{ this }})
{% endif %}
