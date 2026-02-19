{{
    config(
        materialized='incremental',
        unique_key=['invoice_no','stock_code'],
        incremental_strategy='merge',
        on_schema_change='sync_all_columns'
    )
}}

WITH raw_data AS (
    SELECT * 
    FROM {{ source('landingzone', 'RAW_SALES') }}
),

staged AS (
    SELECT
        -- IDs
        CAST(INVOICENO AS VARCHAR)    AS invoice_no,
        CAST(STOCKCODE AS VARCHAR)    AS stock_code,
        CAST(DESCRIPTION AS VARCHAR)  AS description,

        -- Quantities & Prices
        TRY_CAST(QUANTITY AS INT)      AS quantity,
        TRY_CAST(UNITPRICE AS FLOAT)   AS unit_price,
        TRY_CAST(DISCOUNT AS FLOAT)    AS discount,
        TRY_CAST(SHIPPINGCOST AS FLOAT) AS shipping_cost,
        TRY_CAST(CUSTOMERID AS FLOAT)  AS customer_id,

        -- Convert once
        TRY_TO_TIMESTAMP(INVOICEDATE, 'YYYY-MM-DD HH24:MI:SS') AS invoice_date,

        TRIM(UPPER(COUNTRY))            AS country,
        TRIM(UPPER(PAYMENTMETHOD))      AS payment_method,
        TRIM(UPPER(CATEGORY))           AS category,
        TRIM(UPPER(SALESCHANNEL))       AS sales_channel,
        TRIM(UPPER(RETURNSTATUS))       AS return_status,
        TRIM(UPPER(SHIPMENTPROVIDER))   AS shipment_provider,
        TRIM(UPPER(WAREHOUSELOCATION))  AS warehouse_location,
        TRIM(UPPER(ORDERPRIORITY))      AS order_priority,

        CURRENT_TIMESTAMP()             AS stg_loaded_at

    FROM raw_data
),

validated AS (
    SELECT
        *,
        YEAR(invoice_date)  AS invoice_year,
        MONTH(invoice_date) AS invoice_month,

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

SELECT *
FROM validated

{% if is_incremental() %}
WHERE NOT EXISTS (
    SELECT 1
    FROM {{ this }} t
    WHERE t.invoice_year  = validated.invoice_year
      AND t.invoice_month = validated.invoice_month
)
{% endif %}
