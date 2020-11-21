SELECT 
    sku, 
    REPLACE(REPLACE(REPLACE(REPLACE(TRIM(sku_name), '\r', ''),'\n',''),'\t',''),'\"', '') as sku_name,
    category1, 
    category2, 
    brand, 
    available, 
    reserved, 
    total, 
    created_at, 
    updated_at, 
    synched_at
FROM aoi.sku_stock
WHERE \$CONDITIONS