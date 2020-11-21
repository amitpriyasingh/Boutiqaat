SELECT 
    traget_month, 
    brand_target_owner, 
    brand_target_owner_email, 
    brand_target_owner_type, 
    brand, 
    REPLACE(REPLACE(REPLACE(REPLACE(TRIM(celebrity_name), '\r', ''),'\n',''),'\t',''),'\"', '') as celebrity_name, 
    mtd_target, 
    total_target, 
    mtd_sale, 
    monthly_projected_sale, 
    mtd_achievement, 
    monthly_projected_achievement 
FROM aoi.brand_target_sales_master_details
WHERE \$CONDITIONS
