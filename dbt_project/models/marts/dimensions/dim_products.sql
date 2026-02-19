{{
    config(
        materialized='table'
    )
}}

SELECT DISTINCT
    stock_code,
    description,
    category
FROM {{ ref('processed_sales') }}
