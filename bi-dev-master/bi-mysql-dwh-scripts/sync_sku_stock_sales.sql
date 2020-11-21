START TRANSACTION;
INSERT INTO aoi.sku_stock(sku,category1,category2,brand, available, reserved, total, created_at, updated_at, synched_at) select sel.sku, @cat1:=scb.category1, @cat2:=scb.category2, @brand:=scb.brand, @avl:=sel.available, @res:=sel.reserved, @tot:=sel.total, @cdt:=CONVERT_TZ(sel.created_at,'+00:00','+03:00'), @udt:=CONVERT_TZ(sel.updated_at,'+00:00','+03:00'), @sdt:=CONVERT_TZ(sel.synched_at,'+00:00','+03:00')
from stock_entry_log_backup sel
left join sku_cat_brand scb on sel.sku = scb.sku
ON DUPLICATE KEY UPDATE category1 = @cat1, category2 = @cat2, brand = @brand, available = @avl, reserved = @res, total = @tot,  created_at = @cdt, updated_at = @udt, synched_at = @sdt;

REPLACE INTO sku_sales(order_date,sku,sku_name,category1,category2,brand,sold_qtys) select order_date, sku, sku_name, category1, category2, brand, SUM(quantity) sold_qtys from aoi.order_details where order_category != 'CELEBRITY' AND order_status != 'Cancel' AND order_date >= DATE(CONVERT_TZ(now(),'+00:00','+03:00')-INTERVAL 7 DAY) group by order_date, sku;

TRUNCATE TABLE aoi.sku_stock_sales;

INSERT INTO aoi.sku_stock_sales SELECT stock.sku, @sku_name:= COALESCE(sale.sku_name,stock.sku), @cat1 := COALESCE(stock.category1,sale.category1), @cat2 := COALESCE(stock.category2,sale.category2), @brand := COALESCE(stock.brand,sale.brand), @avl := stock.available, @rsv := stock.reserved, @tot := stock.total total_qty, @stock_entry_inserted_at := stock.created_at, @stock_entry_updated_at:= stock.updated_at , @stock_entry_synched_at := stock.synched_at, @sale_entry_synched_at:= sale.synched_at, @report_date := DATE(CONVERT_TZ(now(),'+00:00','+03:00')), @sold_qty_today := sold_qty_today, @sold_qty_yesterday := sold_qty_yesterday, @sold_qty_7days := sold_qty_7days, @sold_qty_14days := sold_qty_14days, @sold_qty_mtd := sold_qty_mtd, @sold_qty_m1 := sold_qty_m1, @sold_qty_m2 := sold_qty_m2, @sold_qty_m3 := sold_qty_m3, @sold_qty_m4 := sold_qty_m4, @sold_qty_m5 := sold_qty_m5 FROM sku_stock stock LEFT JOIN   
(select sku, sku_name, category1, category2, brand, SUM(IF(order_date = today, sold_qtys, 0)) sold_qty_today,
SUM(IF(order_date = yesterday, sold_qtys, 0)) sold_qty_yesterday,
SUM(IF(order_date between (today - INTERVAL 7 DAY) AND yesterday, sold_qtys, 0)) sold_qty_7days,
SUM(IF(order_date between (today - INTERVAL 14 DAY) AND yesterday, sold_qtys, 0)) sold_qty_14days,
SUM(IF(order_date between mtd_first_day AND mtd_last_day, sold_qtys, 0)) sold_qty_mtd,
SUM(IF(order_date between m1_first_day AND m1_last_day, sold_qtys, 0)) sold_qty_m1,
SUM(IF(order_date between m2_first_day AND m2_last_day, sold_qtys, 0)) sold_qty_m2,
SUM(IF(order_date between m3_first_day AND m3_last_day, sold_qtys, 0)) sold_qty_m3,
SUM(IF(order_date between m4_first_day AND m4_last_day, sold_qtys, 0)) sold_qty_m4,
SUM(IF(order_date between m5_first_day AND m5_last_day, sold_qtys, 0)) sold_qty_m5, CONVERT_TZ(MAX(synched_at_utc),'+00:00','+03:00') synched_at FROM 
(select * from sku_sales  WHERE order_date >= DATE_ADD(DATE_ADD(LAST_DAY(LAST_DAY(CONVERT_TZ(now(),'+00:00','+03:00')) - INTERVAL 5 MONTH),INTERVAL 1 DAY),INTERVAL - 1 MONTH)) sale_by_day,
(select today, yesterday, DATE_ADD(DATE_ADD(mtd_last_day,INTERVAL 1 DAY),INTERVAL - 1 MONTH) mtd_first_day, mtd_last_day,
 DATE_ADD(DATE_ADD(m1_last_day,INTERVAL 1 DAY),INTERVAL - 1 MONTH) m1_first_day, m1_last_day,
 DATE_ADD(DATE_ADD(m2_last_day,INTERVAL 1 DAY),INTERVAL - 1 MONTH) m2_first_day, m2_last_day,
 DATE_ADD(DATE_ADD(m3_last_day,INTERVAL 1 DAY),INTERVAL - 1 MONTH) m3_first_day, m3_last_day,
 DATE_ADD(DATE_ADD(m4_last_day,INTERVAL 1 DAY),INTERVAL - 1 MONTH) m4_first_day, m4_last_day,
 DATE_ADD(DATE_ADD(m5_last_day,INTERVAL 1 DAY),INTERVAL - 1 MONTH) m5_first_day, m5_last_day
 FROM (select CONVERT_TZ(now(),'+00:00','+03:00') time_now, date(CONVERT_TZ(now(),'+00:00','+03:00')) today, date(CONVERT_TZ(now(),'+00:00','+03:00') - INTERVAL 1 DAY) yesterday, LAST_DAY(CONVERT_TZ(now(),'+00:00','+03:00')) mtd_last_day, LAST_DAY(LAST_DAY(CONVERT_TZ(now(),'+00:00','+03:00')) - INTERVAL 1 MONTH) m1_last_day, LAST_DAY(LAST_DAY(CONVERT_TZ(now(),'+00:00','+03:00')) - INTERVAL 2 MONTH) m2_last_day, LAST_DAY(LAST_DAY(CONVERT_TZ(now(),'+00:00','+03:00')) - INTERVAL 3 MONTH) m3_last_day, LAST_DAY(LAST_DAY(CONVERT_TZ(now(),'+00:00','+03:00')) - INTERVAL 4 MONTH) m4_last_day, LAST_DAY(LAST_DAY(CONVERT_TZ(now(),'+00:00','+03:00')) - INTERVAL 5 MONTH) m5_last_day) sale_window) sale_window
 group by sku) sale ON stock.sku=sale.sku
 ON DUPLICATE KEY UPDATE sku_name = @sku_name, category1 = @cat1, category2 = @cat2, brand = @brand, stock_qty = @avl, reserved_qty = @rsv, total_qty = @tot , stock_entry_inserted_at = @stock_entry_inserted_at, stock_entry_updated_at = @stock_entry_updated_at, stock_entry_synched_at = @stock_entry_synched_at, sale_entry_synched_at = @sale_entry_synched_at, report_date = @report_date, sold_qty_today = @sold_qty_today, sold_qty_yesterday = @sold_qty_yesterday, sold_qty_7days = @sold_qty_7days, sold_qty_14days = @sold_qty_14days, 
 sold_qty_mtd = @sold_qty_mtd, sold_qty_m1 = @sold_qty_m1, sold_qty_m2 = @sold_qty_m2, sold_qty_m3 = @sold_qty_m3, sold_qty_m4 = @sold_qty_m4, sold_qty_m5 = @sold_qty_m5;

 
COMMIT;

