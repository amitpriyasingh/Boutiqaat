BEGIN;
DELETE FROM aoi.sales_order_items where 1=1;


INSERT INTO aoi.sales_order_items
SELECT
    isl.web_order_no as order_number,
    isl.item_id as item_id,
    batch.batch_id as batch_id,
    (batch.inserted_on+INTERVAL '3 HOURS') as batch_inserted_at,
    isl.item_no as sku,
    scb.sku_name as sku_name,
    scb.category_1 as category_1,
    scb.category_2 as category_2,
    scb.category_3 as category_3,
    scb.category_4 as category_4,
    scb.brand as brand,
    scb.gender as gender,
    scb.color as color,
    scb.size as size,
    scb.unit_cost as unit_cost,
    NULL::varchar(10) as celebrity_code,
    (CASE WHEN ipl.agent_code*1=0 THEN NULL ELSE ipl.agent_code*1 END) as celebrity_id,
    NULL::varchar(50) as celebrity_name,
    NULL::varchar(50) as account_manager,
    isl.quantity as quantity,
    ipl.currency_code as order_currency,
    ipl.currency_factor as currency_factor,
    CAST((1.0/ipl.currency_factor) as DECIMAL(10,3)) as exchange_rate,
    ipl.unit_price as net_sale_price,
    ipl.mrp_price as rrp,
    (ipl.unit_price + ipl.coupon_amount) as list_price,
    (order_shipping_charges * (ipl.unit_price / NULLIF(order_sum_unit_price,0))) as shipping_charge,
    (order_cod_charges * (ipl.unit_price / NULLIF(order_sum_unit_price,0))) as cod_charge,
    CAST((1.0/NULLIF(order_sum_unit_price,0)) as DECIMAL(10,3)) as allocated_order_count,
    date(ish.order_datetime+INTERVAL '3 hours') as order_date,
    (ish.order_datetime+INTERVAL '3 hours') as order_at,
    date(ish.order_datetime) as order_date_utc,
    ish.order_datetime as order_at_utc,
    ish.order_type as order_type,
    ish.order_category as order_category,
    order_payment_method as payment_method,
    order_payment_gateway as payment_gateway,
    ish.customer_id as customer_id,
    ioa.billing_phone_no as billing_phone_no,
    ioa.billing_country as billing_country,
    ioa.shipping_phone_no as shipping_phone_no,
    ioa.shipping_country as shipping_country,
    (ipl.unit_price*NULLIF(CAST((1.0/ipl.currency_factor) as DECIMAL(10,3)),0)) as net_sale_price_kwd,
    (order_shipping_charges * (ipl.unit_price / NULLIF(order_sum_unit_price,0)) *CAST((1.0/ipl.currency_factor) as DECIMAL(10,3))) as shipping_charge_kwd,
    (order_cod_charges * (ipl.unit_price / NULLIF(order_sum_unit_price,0)) *CAST((1.0/ipl.currency_factor) as DECIMAL(10,3))) as cod_charge_kwd,
    ois.status_id as order_status_id,     
    ois.status_name as order_status, 
    ois.updated_on as status_updated_at,
    ish.order_inserted_on_utc as order_inserted_on_utc
FROM
(
    select 
        ish.web_order_no,
        ish.customer_id, 
        COALESCE(ish.payment_method_code, ipl.payment_method_code) order_payment_method,
        COALESCE(ish.payment_gateway, ipl.payment_gateway) order_payment_gateway, 
        ish.order_type, 
        ish.order_category, 
        ish.inserted_on order_inserted_on_utc,
        ish.order_datetime,  
        SUM(CASE ipl.is_header WHEN 1 THEN shipping_charges ELSE 0 END) order_shipping_charges, 
        SUM(CASE ipl.is_header WHEN 1 THEN cod_charges ELSE 0 END) order_cod_charges, 
        SUM(CASE ipl.is_header WHEN 0 THEN unit_price ELSE 0 END) order_sum_unit_price, 
        COUNT(DISTINCT item_id) item_count 
    FROM ofs.inbound_sales_header ish
    LEFT JOIN ofs.inbound_payment_line ipl 
    ON ish.web_order_no = ipl.web_order_no 
    GROUP BY 1,2,3,4,5,6,7,8
) ish
LEFT JOIN ofs.inbound_sales_line isl ON ish.web_order_no = isl.web_order_no
LEFT JOIN aoi.nav_item_sku_master scb ON scb.sku = isl.item_no
LEFT JOIN ofs.inbound_payment_line ipl on ipl.is_header=0 and isl.item_id = ipl.item_id and ipl.web_order_no=isl.web_order_no
LEFT JOIN (select item_id, MAX(batch_id) batch_id, MAX(inserted_on) inserted_on from ofs.order_batch_details group by 1) batch ON batch.item_id = isl.item_id
LEFT JOIN 
(
    SELECT 
        web_order_no, 
        customer_id,
        MAX(CASE 
            WHEN ioa.address_detail_type='Bill' THEN COALESCE(ioa.phone_no,ioa.alternate_phoneno) END) billing_phone_no,
        MAX(CASE WHEN ioa.address_detail_type='Bill' THEN ioa.country END) billing_country,
        MAX(CASE WHEN ioa.address_detail_type='Ship' THEN COALESCE(ioa.phone_no,ioa.alternate_phoneno) END) shipping_phone_no,
        MAX(CASE WHEN ioa.address_detail_type='Ship' THEN ioa.country END) shipping_country
    FROM ofs.inbound_order_address ioa 
    where customer_id is not null 
    GROUP BY 1, 2
) ioa on isl.web_order_no = ioa.web_order_no
LEFT JOIN
(
    select 
	web_order_no, 
	item_id,
	status_id,
	status_name,
	updated_on
    from
    (
        select
            row_number() OVER(PARTITION BY os.item_id ORDER BY os.updated_on DESC, os.id DESC) rank,
            os.web_order_no as web_order_no, 
            os.item_id as item_id,
            os.status_id as status_id, 
            sm.status_name as status_name, 
            os.updated_on updated_on
        from ofs.order_status os
        left join ofs.status_master sm
        ON sm.id=os.status_id 
    )
    where rank=1
) ois
ON ois.item_id=isl.item_id;


COMMIT;





