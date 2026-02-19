-- back compat for old kwarg name
  
  begin;
    
        
            
                
                
            
                
                
            
        
    

    

    merge into INCREMENTALETL._marts.fact_sales as DBT_INTERNAL_DEST
        using INCREMENTALETL._marts.fact_sales__dbt_tmp as DBT_INTERNAL_SOURCE
        on (
                    DBT_INTERNAL_SOURCE.invoice_no = DBT_INTERNAL_DEST.invoice_no
                ) and (
                    DBT_INTERNAL_SOURCE.stock_code = DBT_INTERNAL_DEST.stock_code
                )

    
    when matched then update set
        "INVOICE_NO" = DBT_INTERNAL_SOURCE."INVOICE_NO","STOCK_CODE" = DBT_INTERNAL_SOURCE."STOCK_CODE","CUSTOMER_ID" = DBT_INTERNAL_SOURCE."CUSTOMER_ID","DATE_KEY" = DBT_INTERNAL_SOURCE."DATE_KEY","QUANTITY" = DBT_INTERNAL_SOURCE."QUANTITY","UNIT_PRICE" = DBT_INTERNAL_SOURCE."UNIT_PRICE","DISCOUNT" = DBT_INTERNAL_SOURCE."DISCOUNT","SHIPPING_COST" = DBT_INTERNAL_SOURCE."SHIPPING_COST","GROSS_AMOUNT" = DBT_INTERNAL_SOURCE."GROSS_AMOUNT","NET_AMOUNT" = DBT_INTERNAL_SOURCE."NET_AMOUNT","TOTAL_AMOUNT" = DBT_INTERNAL_SOURCE."TOTAL_AMOUNT","IS_RETURNED" = DBT_INTERNAL_SOURCE."IS_RETURNED","PROCESSED_AT" = DBT_INTERNAL_SOURCE."PROCESSED_AT"
    

    when not matched then insert
        ("INVOICE_NO", "STOCK_CODE", "CUSTOMER_ID", "DATE_KEY", "QUANTITY", "UNIT_PRICE", "DISCOUNT", "SHIPPING_COST", "GROSS_AMOUNT", "NET_AMOUNT", "TOTAL_AMOUNT", "IS_RETURNED", "PROCESSED_AT")
    values
        ("INVOICE_NO", "STOCK_CODE", "CUSTOMER_ID", "DATE_KEY", "QUANTITY", "UNIT_PRICE", "DISCOUNT", "SHIPPING_COST", "GROSS_AMOUNT", "NET_AMOUNT", "TOTAL_AMOUNT", "IS_RETURNED", "PROCESSED_AT")

;
    commit;