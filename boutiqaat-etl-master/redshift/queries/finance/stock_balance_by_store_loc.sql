BEGIN;
DROP TABLE IF EXISTS analytics.stock_balance_by_location_store;

select * into analytics.stock_balance_by_location_store from (
select 
		i.no as sku,
		i.brand,
		i.item_category_code as category,
		v.vendor_name as supplier,
		v.vendor_no as supplier_code,
		nssl.location as location_code,
		case nssl.location_name
			WHEN 'KWIPHOTO' THEN 'Photo'
			WHEN 'KWISHOWROOM' THEN 'Showroom'
			WHEN 'KWIMGMT' THEN 'Management'
			WHEN 'BOUTIQAAT WAREHOUSE19' THEN 'Warehouse'
			ELSE location_name END as location_name,
		nssl.bin_type_code as bin_type_code,
		COALESCE(nssl.quantity,0)  as quantity,
		i.unit_cost,
		CAST(i.unit_cost as DECIMAL(10,3)) * (COALESCE(nssl.quantity,0)) as total_cost_kwd,
		nssl.min_insert_time as min_insert_date,
		nssl.max_insert_time as max_insert_date
	from nav.item i
	left join analytics.nav_sku_stock_location nssl on nssl.sku=i.no
	left join (select iv.item_no as sku, iv.vendor_no as vendor_no, v.name as vendor_name 
				from nav.item_vendor iv left join nav.vendor v ON v.no=iv.vendor_no) v ON v.sku=i.no
	where location_code is not null
);
COMMIT;