SELECT
    order_number, 
    app_order_number,
    item_id, 
    bundle_id, 
    bundle_seq_id, 
    batch_id, 
    awbno, 
    dsp_code, 
    sku,
    REPLACE(REPLACE(REPLACE(REPLACE(TRIM(sku_name), '\r', ''),'\n',''),'\t',''),'\"', '') as sku_name,
    category1,
    category2,
    brand,
    gender,
    celebrity_code,
    celebrity_id,
    REPLACE(REPLACE(REPLACE(REPLACE(TRIM(celebrity_name), '\r', ''),'\n',''),'\t',''),'\"', '') as celebrity_name,
    account_manager,
    quantity,
    order_currency,
    exchange_rate,
    net_sale_price,
    rrp,
    list_price,
    shipping_charge,
    cod_charge,
    allocated_order_count,
    order_date,
    order_at,
    order_date_utc,
    order_at_utc,
    order_type,
    order_category,
    REPLACE(REPLACE(REPLACE(REPLACE(TRIM(payment_method), '\r', ''),'\n',''),'\t',''),'\"', '') as payment_method,
    REPLACE(REPLACE(REPLACE(REPLACE(TRIM(payment_gateway), '\r', ''),'\n',''),'\t',''),'\"', '') as payment_gateway,
    customer_id,
    REPLACE(REPLACE(REPLACE(REPLACE(TRIM(billing_phone_no), '\r', ''),'\n',''),'\t',''),'\"', '') as billing_phone_no,
    REPLACE(REPLACE(REPLACE(REPLACE(TRIM(billing_country), '\r', ''),'\n',''),'\t',''),'\"', '') as billing_country,
    REPLACE(REPLACE(REPLACE(REPLACE(TRIM(shipping_phone_no), '\r', ''),'\n',''),'\t',''),'\"', '') as shipping_phone_no,
    REPLACE(REPLACE(REPLACE(REPLACE(TRIM(shipping_country), '\r', ''),'\n',''),'\t',''),'\"', '') as shipping_country,
    net_sale_price_kwd,
    shipping_charge_kwd,
    cod_charge_kwd,
    order_status_id,
    order_status,
    status_grouped_mgmt,
    status_grouped_ops,
    last_activity,
    status_at,
    cancelled_at,
    confirmed_at,
    readytoship_at,
    picked_at,
    manifested_at, 
    order_allocated_at,
    packed_at,
    shipped_at,
    delivered_at,
    returned_at,
    batch_inserted_at,
    order_inserted_on_utc,
    is_ndr, 
    updated_at_utc
FROM aoi.order_details
WHERE \$CONDITIONS