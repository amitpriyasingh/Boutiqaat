BEGIN;
DROP TABLE IF EXISTS aoi.soh_report;

SELECT * INTO aoi.soh_report
FROM
(SELECT 
    stock.sku as sku,
    stock.store_wh_location as location,
    stock.supplier_name as supplier,
    stock.supplier_item_number as supplier_item_no,
    stock.vendor_code as vendor_code,
    COALESCE(stock.brand_name,sale.brand) as brand,
    stock.sku_2 as sku_2,
    stock.barcode as barcode,
    COALESCE(stock.category_1,sale.category_1) as category_1,
    COALESCE(stock.category_2,sale.category_2) as category_2,
    stock.category_3 as category_3,
    stock.category_4 as category_4,
    COALESCE(stock.item_description,sale.sku_name) as sku_name,
    stock.retail_price as retail_price,
    stock.cost_price as cost_price,
    stock.soh as soh,
    COALESCE(open_po.total_open_quantity,0) as open_po_total_qty,
    COALESCE(open_po.pending_receipt_quantity,0) as open_po_pending_receipt_qty,
    COALESCE(open_po.partially_received_quantity,0) as open_po_partially_received_qty,
    COALESCE(open_po.pending_cancellation_quantity,0) as open_po_pending_cancellation_qty,
    stock.first_grn_date as first_grn_date,
    stock.last_grn_date as last_grn_date,
    stock.payment_terms as payment_term_code,
    stock.country as country,
    (stock.updated_at_utc+INTERVAL '3 HOURS') as stock_entry_synched_at,
    sale.synched_at as sale_entry_synched_at,
    GETDATE()+INTERVAL '3 HOURS' as report_date,
    sale.sold_qty_lifetime as sold_qty_lifetime,
    sale.sold_qty_yesterday as sold_qty_yesterday,
    sale.sold_qty_7days as sold_qty_7days,
    sale.sold_qty_14days as sold_qty_14days,
    sale.sold_qty_mtd as sold_qty_mtd,
    sale.sold_qty_m1 as sold_qty_m1,
    sale.sold_qty_m2 as sold_qty_m2,
    sale.sold_qty_m3 as sold_qty_m3,
    sale.sold_qty_m4 as sold_qty_m4,
    sale.sold_qty_m5 as sold_qty_m5
FROM (select * from aoi.soh_entry_log where soh > 0) stock
    LEFT JOIN
    (
        select 
            sku, 
            sku_name, 
            category1 as category_1, 
            category2 as category_2, 
            brand, 
            SUM(CASE WHEN order_date <= yesterday THEN sold_qtys ELSE 0 END) sold_qty_lifetime,
            SUM(CASE WHEN order_date = yesterday THEN sold_qtys ELSE 0 END) sold_qty_yesterday,
            SUM(CASE WHEN order_date between (today - INTERVAL '7 DAY') AND yesterday THEN sold_qtys ELSE 0 END) sold_qty_7days,
            SUM(CASE WHEN order_date between (today - INTERVAL '14 DAY') AND yesterday THEN sold_qtys ELSE 0 END) sold_qty_14days,
            SUM(CASE WHEN order_date between mtd_first_day AND yesterday THEN sold_qtys ELSE 0 END) sold_qty_mtd,
            SUM(CASE WHEN order_date between m1_first_day AND m1_last_day THEN sold_qtys ELSE 0 END) sold_qty_m1,
            SUM(CASE WHEN order_date between m2_first_day AND m2_last_day THEN sold_qtys ELSE 0 END) sold_qty_m2,
            SUM(CASE WHEN order_date between m3_first_day AND m3_last_day THEN sold_qtys ELSE 0 END) sold_qty_m3,
            SUM(CASE WHEN order_date between m4_first_day AND m4_last_day THEN sold_qtys ELSE 0 END) sold_qty_m4,
            SUM(CASE WHEN order_date between m5_first_day AND m5_last_day THEN sold_qtys ELSE 0 END) sold_qty_m5, 
            MAX(synched_at_utc)+INTERVAL '3 HOURS' as synched_at 
        FROM aoi.ofs_sku_sales,
        (
            select 
                today, 
                yesterday, 
                date(DATEADD(mon,-1,DATEADD(day,1,mtd_last_day))) mtd_first_day, 
                date(mtd_last_day) mtd_last_day,
                date(DATEADD(mon,-1,DATEADD(day,1,m1_last_day))) m1_first_day, 
                date(m1_last_day) m1_last_day,
                date(DATEADD(mon,-1,DATEADD(day,1,m2_last_day))) m2_first_day, 
                date(m2_last_day) m2_last_day,
                date(DATEADD(mon,-1,DATEADD(day,1,m3_last_day))) m3_first_day, 
                date(m3_last_day) m3_last_day,
                date(DATEADD(mon,-1,DATEADD(day,1,m4_last_day))) m4_first_day, 
                date(m4_last_day) m4_last_day,
                date(DATEADD(mon,-1,DATEADD(day,1,m5_last_day))) m5_first_day, 
                date(m5_last_day) m5_last_day
            FROM 
            (
            	select 
	            	GETDATE()+INTERVAL '3 hours' as time_now, 
					date(GETDATE()+INTERVAL '3 hours') today, 
					date((GETDATE()+INTERVAL '3 hours') - INTERVAL '1 DAY') yesterday, 
					LAST_DAY(GETDATE()+INTERVAL '3 hours') mtd_last_day, 
					LAST_DAY(DATEADD(mon,-1,date(GETDATE()+INTERVAL '3 hours'))) m1_last_day,
					LAST_DAY(DATEADD(mon,-2,date(GETDATE()+INTERVAL '3 hours'))) m2_last_day,
					LAST_DAY(DATEADD(mon,-3,date(GETDATE()+INTERVAL '3 hours'))) m3_last_day,
					LAST_DAY(DATEADD(mon,-4,date(GETDATE()+INTERVAL '3 hours'))) m4_last_day,
					LAST_DAY(DATEADD(mon,-5,date(GETDATE()+INTERVAL '3 hours'))) m5_last_day
            )sale_window
        ) sale_window
        group by sku, sku_name, category_1, category_2, brand
        having sold_qty_lifetime > 0
    ) sale ON stock.sku=sale.sku
    LEFT JOIN 
    (
        SELECT 
            sku, 
            SUM(COALESCE(total_open_quantity,0)) as total_open_quantity, 
            SUM(COALESCE(pending_receipt_quantity,0)) as pending_receipt_quantity, 
            SUM(COALESCE(partially_received_quantity,0)) as partially_received_quantity, 
            SUM(COALESCE(pending_cancellation_quantity,0)) as pending_cancellation_quantity 
        FROM aoi.open_po 
        where total_open_quantity > 0
        GROUP BY sku 
    ) open_po ON open_po.sku = stock.sku);
COMMIT;