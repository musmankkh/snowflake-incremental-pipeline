

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

FROM INCREMENTALETL._processed.processed_sales


WHERE processed_at > (SELECT MAX(processed_at) FROM INCREMENTALETL._marts.fact_sales)
