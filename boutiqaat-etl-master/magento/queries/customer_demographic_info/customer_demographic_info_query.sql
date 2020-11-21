select 
    cgf.entity_id as customer_id,
    CASE WHEN cgf.gender=2 THEN 'women' 
        WHEN cgf.gender=1 THEN 'men'  
        ELSE 'other'  
    END as gender,
    cgf.dob,
    CASE WHEN cgf.email  is  NULL 
            THEN (select REPLACE(REPLACE(REPLACE(REPLACE(TRIM(so.customer_email), '\r', ''),'\n',''),'\t',''),'\"', '') from boutiqaat_v2.sales_order as so  where so.customer_id= cgf.entity_id limit 1)  
        ELSE REPLACE(REPLACE(REPLACE(REPLACE(TRIM(cgf.email), '\r', ''),'\n',''),'\t',''),'\"', '') 
    END as email,
    CASE WHEN cgf.billing_telephone  is  NULL 
            THEN (select REPLACE(REPLACE(REPLACE(REPLACE(TRIM(so.telephone), '\r', ''),'\n',''),'\t',''),'\"', '') from boutiqaat_v2.sales_order as so  where so.customer_id= cgf.entity_id limit 1)  
        ELSE REPLACE(REPLACE(REPLACE(REPLACE(TRIM(cgf.billing_telephone), '\r', ''),'\n',''),'\t',''),'\"', '')
    END as phone_number  
FROM boutiqaat_v2.customer_grid_flat as cgf
WHERE \$CONDITIONS