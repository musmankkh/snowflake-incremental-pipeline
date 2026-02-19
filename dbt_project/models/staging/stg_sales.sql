{{
    config(
        materialized='incremental',
        unique_key='INVOICENO',
        incremental_strategy='merge',
        on_schema_change='sync_all_columns'
    )
}}

WITH raw_data AS (
    SELECT * FROM {{ source('landingzone', 'RAW_SALES') }}
),

staged AS (
    SELECT
        -- ── IDs ────────────────────────────────────────────────
        CAST(INVOICENO       AS VARCHAR)    AS invoice_no,
        CAST(STOCKCODE       AS VARCHAR)    AS stock_code,
        CAST(DESCRIPTION     AS VARCHAR)    AS description,

        -- ── Quantities & Prices ────────────────────────────────
        TRY_CAST(QUANTITY    AS INT)        AS quantity,
        TRY_CAST(UNITPRICE   AS FLOAT)      AS unit_price,
        TRY_CAST(DISCOUNT    AS FLOAT)      AS discount,
        TRY_CAST(SHIPPINGCOST AS FLOAT)     AS shipping_cost,
        TRY_CAST(CUSTOMERID  AS FLOAT)      AS customer_id,

        -- ── Date Fix: VARCHAR → proper TIMESTAMP ───────────────
        -- Handles format: '2020-01-01 00:00:00'
        TRY_TO_TIMESTAMP(INVOICEDATE, 'YYYY-MM-DD HH24:MI:SS') AS invoice_date,
        YEAR(TRY_TO_TIMESTAMP(INVOICEDATE, 'YYYY-MM-DD HH24:MI:SS'))  AS invoice_year,
        MONTH(TRY_TO_TIMESTAMP(INVOICEDATE, 'YYYY-MM-DD HH24:MI:SS')) AS invoice_month,

        -- ── Categoricals ───────────────────────────────────────
        TRIM(UPPER(COUNTRY))         AS country,
        TRIM(UPPER(PAYMENTMETHOD))   AS payment_method,
        TRIM(UPPER(CATEGORY))        AS category,
        TRIM(UPPER(SALESCHANNEL))    AS sales_channel,
        TRIM(UPPER(RETURNSTATUS))    AS return_status,
        TRIM(UPPER(SHIPMENTPROVIDER)) AS shipment_provider,
        TRIM(UPPER(WAREHOUSELOCATION)) AS warehouse_location,
        TRIM(UPPER(ORDERPRIORITY))   AS order_priority,

        -- ── Audit columns ──────────────────────────────────────
        CURRENT_TIMESTAMP()          AS stg_loaded_at

    FROM raw_data
),

-- ── Data Quality Flags ────────────────────────────────────────
validated AS (
    SELECT
        *,
        CASE
            WHEN invoice_date IS NULL           THEN 'INVALID_DATE'
            WHEN quantity IS NULL               THEN 'INVALID_QUANTITY'
            WHEN unit_price IS NULL             THEN 'INVALID_PRICE'
            WHEN shipping_cost < 0              THEN 'NEGATIVE_SHIPPING'
            WHEN discount < 0 OR discount > 1   THEN 'INVALID_DISCOUNT'
            WHEN invoice_no IS NULL             THEN 'NULL_INVOICE'
            ELSE 'VALID'
        END AS data_quality_flag

    FROM staged
)

SELECT * FROM validated

{% if is_incremental() %}
    -- ── Incremental: only process new months not already in staging ──
    WHERE (invoice_year, invoice_month) NOT IN (
        SELECT DISTINCT invoice_year, invoice_month
        FROM {{ this }}
    )
{% endif %}