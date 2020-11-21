BEGIN;
DROP TABLE IF EXISTS analytics.nav_items_qty;
select * into analytics.nav_items_qty 
from(
	select 
		location_code as location, 
		sellable_location.location_name as location_name,
		bin_type_code, 
		bin_code, 
		item_no as sku, 
		SUM(COALESCE(quantity,0)) as quantity, 
		MIN(insert_datetime)  min_insert_time,
		MAX(insert_datetime)  max_insert_time 
	from nav.warehouse_entry
	Join (select code as location, name as location_name from nav.location where bin_mandatory=1) as sellable_location ON sellable_location.location=location_code
	GROUP BY 1,2,3,4,5
);
COMMIT;