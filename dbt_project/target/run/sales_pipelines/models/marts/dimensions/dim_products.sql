
  
    

create or replace transient table INCREMENTALETL._marts.dim_products
    
    
    
    as (

SELECT DISTINCT
    stock_code,
    description,
    category
FROM INCREMENTALETL._processed.processed_sales
    )
;


  