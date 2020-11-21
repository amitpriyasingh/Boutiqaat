BEGIN;
DROP TABLE IF EXISTS analytics.nav_stock_details_entry;

select * into analytics.nav_stock_details_entry 
from(
select sku, max_entry_date, qty_in_stock, wh_entry, wh_jn_line, wh_activity_line, wh_activity_line2, move_jn_line
from (select 
	ROW_NUMBER () OVER( PARTITION BY item_no ORDER BY insert_at desc) as rank,
	item_no as sku,
	insert_at as max_entry_date,
	qty_in_stock,
	warehouse_entry as wh_entry,
    warehouse_jn as wh_jn_line,
	warehouse_activity_line as wh_activity_line,
	warehouse_activity_line_2 as wh_activity_line2,
    movement_journal_line move_jn_line 
from nav.stock_details) where rank=1
);
COMMIT;