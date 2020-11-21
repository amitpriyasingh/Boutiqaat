BEGIN;
DROP TABLE IF EXISTS aoi.sku_not_sellable;

SELECT * INTO aoi.sku_not_sellable
FROM
(select * from 
(select *  from analytics.nav_not_sellable_qty
union 
select 
	sku, 
	location, 
	'shipping' as not_sellable_reason, 
	min(min_insert_time) as min_insert_time, 
	max(max_insert_time) as max_insert_time, 
	sum(quantity) as qty 
from analytics.nav_items_qty 
where bin_type_code='SHIP'
group by 1,2,3)
);
COMMIT;