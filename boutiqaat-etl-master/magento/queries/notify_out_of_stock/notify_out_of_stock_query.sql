SELECT
    id,
    entity_id,
    REPLACE(REPLACE(REPLACE(REPLACE(TRIM(email_id), '\r', ''),'\n',''),'\t',''),'\"', '') as email_id,
    notified_to_customer,
    store,
    updated_at,
    created_at,
    synced_to_erp,
    sms_notified,
    category_id,
    REPLACE(REPLACE(REPLACE(REPLACE(TRIM(name), '\r', ''),'\n',''),'\t',''),'\"', '') as name,
    REPLACE(REPLACE(REPLACE(REPLACE(TRIM(mobile), '\r', ''),'\n',''),'\t',''),'\"', '') as mobile,
    sku,
    REPLACE(REPLACE(REPLACE(REPLACE(TRIM(product_name), '\r', ''),'\n',''),'\t',''),'\"', '') as product_name,
    country_code
FROM boutiqaat_v2.notify_out_of_stock
WHERE \$CONDITIONS