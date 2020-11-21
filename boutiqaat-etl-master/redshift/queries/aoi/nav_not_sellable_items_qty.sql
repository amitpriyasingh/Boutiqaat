BEGIN;
DROP TABLE IF EXISTS analytics.nav_not_sellable_qty;

/* create a table */
select * into analytics.nav_not_sellable_qty
FROM
(select * from (SELECT 
	item_no as sku,
	location_code as location, 
	unsellable_location.location_name as not_sellable_reason,
    MIN(posting_date)  min_insert_time,
    MAX(posting_date)  max_insert_time,
    SUM(COALESCE(quantity,0)) as not_sellable_qty
from nav.item_ledger_entry
Join (select code as location, name as location_name from nav.location WHERE bin_mandatory=0) unsellable_location ON unsellable_location.location=location_code
GROUP BY 1,2,3
having SUM(COALESCE(quantity,0)) > 0)
union
select * from (select 
    sku,
    location,
    (CASE 
            WHEN bin_type_code='RECEIVE' THEN 'recieving'
            WHEN bin_type_code='QC' AND bin_code<> 'STAGEMOVEBIN' THEN 'qc_stage'
            ELSE NULL END) not_sellable_reason,
    min_insert_time,
    max_insert_time,
    SUM(
        (CASE 
            WHEN bin_type_code='RECEIVE' THEN coalesce(quantity,0)
            WHEN bin_type_code='QC' AND bin_code<> 'STAGEMOVEBIN' THEN coalesce(quantity,0) 
            ELSE 0 END)) not_sellable_qty 
FROM (select 
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
	GROUP BY 1,2,3,4,5)
GROUP BY 1,2,3,4,5
having not_sellable_reason is not null));
COMMIT;