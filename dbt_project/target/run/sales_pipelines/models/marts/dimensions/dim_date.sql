
  
    

create or replace transient table INCREMENTALETL._marts.dim_date
    
    
    
    as (

SELECT DISTINCT
    invoice_date::DATE AS date_key,
    YEAR(invoice_date)  AS year,
    MONTH(invoice_date) AS month,
    DAY(invoice_date)   AS day,
    DAYNAME(invoice_date) AS day_name,
    WEEK(invoice_date)  AS week_number,
    QUARTER(invoice_date) AS quarter
FROM INCREMENTALETL._processed.processed_sales
    )
;


  