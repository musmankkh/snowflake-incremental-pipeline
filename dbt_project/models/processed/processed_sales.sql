{{
    config(
        materialized='incremental',
        unique_key='invoice_no',
        incremental_strategy='merge',
        on_schema_change='sync_all_columns'
    )
}}

WITH staging AS (
    SELECT * FROM {{ ref('stg_sales') }}
),

-- ── Step 1: Remove duplicates (keep latest loaded record) ────────
deduped AS (
    SELECT *,
        ROW_NUMBER() OVER (
            PARTITION BY invoice_no
            ORDER BY stg_loaded_at DESC
        ) AS row_num
    FROM staging
),

-- ── Step 2: Keep only VALID records, remove bad data ────────────
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

        -- ── Derived / Enriched columns ──────────────────────────
        ROUND(quantity * unit_price, 2)                        AS gross_amount,
        ROUND(quantity * unit_price * (1 - discount), 2)       AS net_amount,
        ROUND(quantity * unit_price * (1 - discount)
              + COALESCE(shipping_cost, 0), 2)                 AS total_amount,

        CASE
            WHEN return_status = 'RETURNED' THEN TRUE
            ELSE FALSE
        END AS is_returned,

        CASE
            WHEN customer_id IS NULL THEN TRUE
            ELSE FALSE
        END AS is_guest_customer,

        -- ── Audit ────────────────────────────────────────────────
        stg_loaded_at,
        CURRENT_TIMESTAMP() AS processed_at

    FROM deduped
    WHERE
        row_num = 1                     -- remove duplicates
        AND data_quality_flag = 'VALID' -- only clean records
        AND quantity > 0                -- remove returns/negatives
        AND unit_price > 0              -- remove invalid prices
        AND invoice_date IS NOT NULL    -- must have valid date
),

-- ── Step 3: Final output ─────────────────────────────────────────
final AS (
    SELECT * FROM cleaned
)

SELECT * FROM final

{% if is_incremental() %}
    -- ── Incremental: only process months not already in processed ──
    WHERE (invoice_year, invoice_month) NOT IN (
        SELECT DISTINCT invoice_year, invoice_month
        FROM {{ this }}
    )
{% endif %}