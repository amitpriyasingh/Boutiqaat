SELECT 
    id, 
    order_temp_id, 
    quantity, 
    product_id, 
    parent_product_id, 
    unit_price, 
    total_price, 
    REPLACE(REPLACE(REPLACE(REPLACE(TRIM(name), '\r', ''),'\n',''),'\t',''),'\"', '') as name, 
    sku, 
    celebrity_id, 
    date_format(DATE_ADD(from_unixtime(0), interval created_time second), '%Y-%m-%d %H:%i:%s') as created_time, 
    date_format(DATE_ADD(from_unixtime(0), interval last_updated second), '%Y-%m-%d %H:%i:%s') as last_updated, 
    tv_id, 
    image_url, 
    discounted_price, 
    is_foc
FROM boutiqaat_middlelayer.order_item_queue
WHERE \$CONDITIONS AND created_time >= UNIX_TIMESTAMP(DATE_ADD(CURDATE(),INTERVAL -20 DAY)) 

