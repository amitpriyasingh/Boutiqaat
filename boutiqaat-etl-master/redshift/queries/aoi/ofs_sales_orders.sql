BEGIN;
DROP TABLE IF EXISTS analytics.ofs_orders;

select * into analytics.ofs_orders
FROM
(select 
	isl.web_order_no as order_no, 
	isl.item_no as sku, 
	isl.product_id as product_id,
	isl.quantity as quantity,
	isl.is_processed as is_processed,
	isl.inserted_on as created_at,
	co.order_datetime as order_datetime,
	sm.status_name as order_status,
	CASE
		WHEN co.order_category='CELEBRITY' THEN 1 
		ELSE 0 
		END as is_celebrity_order
FROM ofs.inbound_sales_line isl
LEFT JOIN ofs.crm_orders co ON co.web_order_no = isl.web_order_no
LEFT JOIN ofs.status_master sm ON sm.id = co.order_status
order by 1);
COMMIT;
