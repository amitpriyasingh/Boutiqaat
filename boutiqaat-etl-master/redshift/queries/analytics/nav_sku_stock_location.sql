BEGIN;
DROP TABLE IF EXISTS analytics.nav_sku_stock_location;

select * into analytics.nav_sku_stock_location
FROM
(
-- items in wh location in (KWI01, BOR)
select 
    location_code as location,
    sellable_location.location_name as location_name, 
    bin_type_code, 
    bin_code, 
    item_no as sku,
    CAST(SUM(COALESCE(quantity,0)) as DECIMAL(10,3)) quantity,
    TRUNC(MIN(insert_datetime))  min_insert_time,
    TRUNC(MAX(insert_datetime))  max_insert_time
from nav.warehouse_entry
Inner Join 
(
    select 
        code as location,
        name as location_name
    from nav.location
    where bin_mandatory=1
) as sellable_location 
ON sellable_location.location=location_code
GROUP BY 1,2,3,4,5
union
(SELECT 
    location_code as location,
    unsellable_location.location_name as location_name,
    NULL as bin_type_code, 
    NULL as bin_code, 
    item_no sku, 
    CAST(SUM(COALESCE(quantity,0)) as DECIMAL(10,3)) quantity,
    TRUNC(MIN(posting_date))  min_posting_time,
    TRUNC(MAX(posting_date))  max_posting_time
from nav.item_ledger_entry
Inner Join 
(
    select 
        code as location,
        name as location_name
    from nav.location
    WHERE bin_mandatory=0
)unsellable_location 
ON unsellable_location.location=location_code
GROUP BY 1,2,3,4,5
having SUM(COALESCE(quantity,0)) > 0)
);
COMMIT;