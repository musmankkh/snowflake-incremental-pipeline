

WITH staging AS (
    SELECT * 
    FROM INCREMENTALETL._staging.stg_sales
),

deduped AS (
    SELECT *,
        ROW_NUMBER() OVER (
            PARTITION BY invoice_no, stock_code
            ORDER BY stg_loaded_at DESC
        ) AS row_num
    FROM staging
),

cleaned AS (
    SELECT
        invoice_no,
        stock_code,
        description,
        quantity,
        unit_price,
        discount,
        shipping_cost,
        customer_id,
        invoice_date,
        invoice_year,
        invoice_month,
        country,
        payment_method,
        category,
        sales_channel,
        return_status,
        shipment_provider,
        warehouse_location,
        order_priority,

        -- Derived metrics
        ROUND(quantity * unit_price, 2) AS gross_amount,
        ROUND(quantity * unit_price * (1 - discount), 2) AS net_amount,
        ROUND(
            quantity * unit_price * (1 - discount)
            + COALESCE(shipping_cost, 0),
        2) AS total_amount,

        CASE WHEN return_status = 'RETURNED' THEN TRUE ELSE FALSE END AS is_returned,
        CASE WHEN customer_id IS NULL THEN TRUE ELSE FALSE END AS is_guest_customer,

        stg_loaded_at,
        CURRENT_TIMESTAMP() AS processed_at

    FROM deduped
    WHERE
        row_num = 1
        AND data_quality_flag = 'VALID'
        AND invoice_date IS NOT NULL
)

SELECT *
FROM cleaned


WHERE NOT EXISTS (
    SELECT 1
    FROM INCREMENTALETL._processed.processed_sales t
    WHERE t.invoice_year  = cleaned.invoice_year
      AND t.invoice_month = cleaned.invoice_month
)
