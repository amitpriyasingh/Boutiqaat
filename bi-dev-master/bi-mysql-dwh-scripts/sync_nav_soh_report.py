#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sat Apr 20 19:18:02 2019

@author: muthu
"""
#import time
from sqlalchemy import create_engine
nav_conn = create_engine("mssql+pymssql://usr1:pass1@0.0.0.1:1455/db1")
ofs_conn = create_engine("mysql+pymysql://usr2:pass2@0.0.0.2/db2")
try:
    soh_select_query = """
    select distinct [VendorName] as [Supplier Name] ,
     [Vendor Item No_] as [Supplier Item number] , [Vendor No_] as [Vendor Code],
     [AttributeDesc] as [Brand Name], [ItemNo]  as [SKU No.], [SKU No2.] as [SKU No2.], [Bar code] as [Barcode],
     [Item Category Code] as [Category 1], [Product Group Code] as [Category 2],
    [3rd Category] as [Category 3],[4th Category] as [Category 4],[ItemDescription] as [Item Description],
    (select     CONVERT(DECIMAL(10,3),[Unit Price])    from  [Boutiqaat Kuwait$Sales Price] WITH (NOLOCK)  where [Ending Date] = '1753-01-01 00:00:00.000' and [Item No_]=ItemNo) as [Retail Price]
    ,CONVERT(DECIMAL(10,3),[TotQuantity]) as [SOH],
     (select  top (1) CONVERT(DECIMAL(10,3), Cost)  from [dbo].[Boutiqaat Kuwait$Item Vendor Discount]   where Blocked=0 and [Vendor No_]=Final.[Vendor No_] and [Item No_]=Final.[ItemNo] order by [Start Date] desc   ) as [Cost Price]
    ,CONVERT(DATE,[FirstGrnDate]) as [First GRN Date],CONVERT(DATE,[LastGrnDate]) as [Last GRN Date],[Payment Terms Code] as [Payment terms],[Country_Region Code] as [Country], [Location] as [Store/WH location]
    from (
     Select *
    from (
    /* Take All Location With All Items Combination
    */
    select  [Boutiqaat Kuwait$Location].[Code] as [Location]
    ,[Boutiqaat Kuwait$Item].[No_] , [Boutiqaat Kuwait$Item].[No_ 2] as [SKU No2.], [Boutiqaat Kuwait$Item].[EAN Code] as [Bar code],[Brand],[Boutiqaat Kuwait$Item].[Product Group Code]
     ,[Boutiqaat Kuwait$Item].[Item Category Code],[Boutiqaat Kuwait$Item].[3rd Category]
     ,[Boutiqaat Kuwait$Item].[4th Category],[Boutiqaat Kuwait$Item].[Description]as ItemDescription
     from [dbo].[Boutiqaat Kuwait$Location] WITH (NOLOCK)
    Cross Join [dbo].[Boutiqaat Kuwait$Item] where [Boutiqaat Kuwait$Location].[Bin Mandatory]=1 )as Tbl1
    /*  Take Data from Whare house Entry and map to Above Tb1*/
    left Join
     (select  Sum( COALESCE([Quantity],0) ) [TotQuantity],[Location Code],[Item No_]as ItemNo from [dbo].[Boutiqaat Kuwait$Warehouse Entry]
     WITH (NOLOCK)  where
      [Boutiqaat Kuwait$Warehouse Entry].[Bin Type Code]<>'SHIP'   Group by  [Location Code],[Item No_] having Sum( COALESCE([Quantity],0) ) > 0 ) as Tbl2
     on  [Tbl2].[Location Code]=Tbl1.[Location] and [Tbl2].[ItemNo]= [Tbl1].[No_]
    /* Take Info and Map with Vendore and Vendor Items */

    left  Join(
     select [Boutiqaat Kuwait$Vendor].[Name]as VendorName,[Boutiqaat Kuwait$Item Vendor].[timestamp],[Vendor Item No_] ,[Boutiqaat Kuwait$Vendor].[Payment Terms Code],
     [Boutiqaat Kuwait$Vendor].[Country_Region Code],[Boutiqaat Kuwait$Item Vendor].[Item No_],[Boutiqaat Kuwait$Item Vendor].[Vendor No_]
     from  [dbo].[Boutiqaat Kuwait$Vendor] WITH (NOLOCK)
     left Join
    [Boutiqaat Kuwait$Item Vendor] WITH (NOLOCK)   on [Boutiqaat Kuwait$Vendor].[No_]=[Boutiqaat Kuwait$Item Vendor].[Vendor No_]) as VenDetails
    on VenDetails.[Item No_]=[Tbl1].[No_]   and VenDetails.[timestamp]= (select max([timestamp]) from [Boutiqaat Kuwait$Item Vendor] WITH (NOLOCK) where [Item No_]= [Tbl1].[No_] )
    left join
    ( select  [Boutiqaat Kuwait$Item Attribute Master]. [Description] as AttributeDesc  ,[Code]  from  [Boutiqaat Kuwait$Item Attribute Master] WITH (NOLOCK) where [Attribute Type]=1
     ) as AttributeMaster on AttributeMaster.Code=[Tbl1].[Brand]) as Final
     left Join (Select  min([Posting Date]) FirstGrnDate, max([Posting Date])
            LastGrnDate ,[Item No_] as lgItemNo from [Boutiqaat Kuwait$Item Ledger Entry] WITH (NOLOCK)  WHERE [Entry Type]=0 and   [Document Type] = 5 GROUP BY [Item No_]
    )as ILE on ILE.lgItemNo= Final.ItemNo
    """
    ResultProxy = nav_conn.execute(soh_select_query)
    sku_location_records = ResultProxy.fetchall()
    try:
        ofs_conn.execute("TRUNCATE TABLE aoi.soh_entry_log")
    except:
        raise Exception("Couldn't empty soh_entry_log table for the next refresh")

    chunk_size = 100
    for i in range(0, len(sku_location_records), chunk_size):
        sku_location_values = sku_location_records[i:i+chunk_size]
        insert_query="INSERT IGNORE INTO aoi.soh_entry_log(supplier, supplier_item_no, vendor_code, brand, sku, sku2, barcode, category1, category2, category3, category4,  sku_name, retail_price, soh, cost_price, first_grn_date, last_grn_date, payment_term_code, country,location) values "
        insert_query = insert_query + str(sku_location_values[0]).replace("%","%%").replace("Decimal('","").replace("')","").replace("None","NULL")+"')"
        for value_row in sku_location_values[1:]:
            insert_query = insert_query + ', ' + str(value_row).replace("%","%%").replace("Decimal('","").replace("')","").replace("None","NULL")+"')"

        try:
            ofs_conn.execute(insert_query)
            print("Inserted "+ str(i+chunk_size) + " rows into soh_entry_log table")
        except:
            raise Exception("Couldn't load data into recent soh_entry_log table!")


except:
    raise Exception("Couldn't query SOH data from NAV DB!")

try:
    open_po_table_select_query="""
    SELECT ph.[No_] AS po_number, ph.[Status] AS status_code, (CASE ph.[Status] WHEN 0 THEN 'Open' WHEN 1 THEN 'Released' WHEN 2 THEN 'Pending Approval' WHEN 3 THEN 'Pending Prepayment' WHEN 4 THEN 'Cancelled' WHEN 5 THEN 'Short Close' WHEN 6 THEN 'Pending Cancellation' WHEN 7 THEN 'Closed' ELSE 'Other' END) status_name,
    ph.[PO Status] po_status_code, (CASE WHEN ph.[Status] = 3 OR (ph.[Status] = 1 AND ph.[PO Status] = 1) THEN 'Open PO - pending receipt' WHEN ph.[Status] = 1 AND ph.[PO Status] = 2 THEN 'Open PO - partially received' WHEN ph.[Status] = 6 THEN 'Open PO - pending cancellation' ELSE (CASE ph.[Status] WHEN 0 THEN 'Open' WHEN 1 THEN 'Released' WHEN 2 THEN 'Pending Approval' WHEN 3 THEN 'Pending Prepayment' WHEN 4 THEN 'Cancelled' WHEN 5 THEN 'Short Close' WHEN 6 THEN 'Pending Cancellation' WHEN 7 THEN 'Closed' ELSE 'Other' END) END) po_status_name,
    CONVERT(DATE,ph.[Order Date]) po_date, ph.[Payment Terms Code] payment_terms_code, CONVERT(DATE,ph.[Expiry Date]) po_expiry_date, (CASE WHEN ph.[Status] IN (1,3,6) THEN 1 ELSE  0 END) open_for_GRN, pl.[No_] AS sku, pl.[Description] AS sku_name, CONVERT(DECIMAL(10,3),SUM(COALESCE(pl.[Quantity],0))) AS total_quantity, CONVERT(DECIMAL(10,3),SUM(COALESCE(pl.[Outstanding Quantity],0))) AS open_quantity
    FROM [dbo].[Boutiqaat Kuwait$Purchase Header] ph WITH (NOLOCK) LEFT JOIN [dbo].[Boutiqaat Kuwait$Purchase Line] pl WITH (NOLOCK) ON ph.[No_] = pl.[Document No_]
    GROUP BY pl.[Document No_], pl.[No_], pl.[Description], ph.[No_],ph.[Status], ph.[Order Date], ph.[Payment Terms Code], ph.[Expiry Date], ph.[PO Status]
    """
    ResultProxy = nav_conn.execute(open_po_table_select_query)
    open_po_records  = ResultProxy.fetchall()
    try:
        ofs_conn.execute("TRUNCATE TABLE aoi.open_po_log")
    except:
        raise Exception("Couldn't empty open_po_log table for the next refresh")

    chunk_size = 50
    for i in range(0, len(open_po_records), chunk_size):
        open_po_records_values = open_po_records[i:i+chunk_size]
        insert_query="INSERT IGNORE INTO aoi.open_po_log(po_number, status_code, status_name, po_status_code, po_status_name, po_date, payment_terms_code, po_expiry_date, open_for_GRN, sku, sku_name, total_quantity,open_quantity) values"
        insert_query = insert_query + str(open_po_records_values[0]).replace("%","%%").replace("Decimal('","").replace("')","").replace("None","NULL")
        for value_row in open_po_records_values[1:]:
            insert_query = insert_query + ', ' + str(value_row).replace("%","%%").replace("Decimal('","").replace("')","").replace("None","NULL")
        try:
            insert_query=str(insert_query.encode('ascii', 'xmlcharrefreplace'), encoding='ascii')
            ofs_conn.execute(insert_query)
            print("Inserted "+ str(i+chunk_size) + " rows into aoi.open_po_log")
        except:
            raise Exception("Couldn't load data into recent open_po_log table!")
    
    try:
        ofs_conn.execute("TRUNCATE TABLE aoi.open_po")
    except:
        raise Exception("Couldn't empty open_po table for the next refresh")

    try:
        open_po_insert_query="""
        REPLACE INTO aoi.open_po(po_number, po_status_code, po_status_name, po_date, payment_terms_code, po_expiry_date, open_for_GRN, sku, sku_name, total_quantity,total_open_quantity, pending_receipt_quantity, partially_received_quantity, pending_cancellation_quantity, synched_at) select @po_number:=po_number, @po_status_code:=po_status_code, @po_status_name:=po_status_name, @po_date:=po_date, @payment_terms_code:=payment_terms_code, @po_expiry_date:=po_expiry_date, @open_for_GRN:=open_for_GRN, @sku:=sku, @sku_name:=sku_name, @total_quantity:=total_quantity, @total_open_quantity:=SUM(IF(open_for_GRN=1, open_quantity, 0)),
        @pending_receipt_quantity:= SUM(IF(po_status_name='Open PO - pending receipt' AND open_for_GRN=1, open_quantity, 0)), @partially_received_quantity:=SUM(IF(po_status_name='Open PO - partially received' AND open_for_GRN=1, open_quantity, 0)), @pending_cancellation_quantity:=SUM(IF(po_status_name='Open PO - pending cancellation' AND open_for_GRN=1, open_quantity, 0)),
        @synched_at:=(updated_at_utc+INTERVAL 3 HOUR) from aoi.open_po_log
        GROUP BY po_number, sku
        """
        ofs_conn.execute(open_po_insert_query)
    except:
        raise Exception("Couldn't upsert data into open_po table!")

except:
    raise Exception("Couldn't query PO tables from NAV DB!")

#time.sleep(60) #Delay for a minute between creating open_po & soh_report tables
try:
    upsert_soh_report_query="""
    REPLACE INTO aoi.soh_report(sku,location,supplier,supplier_item_no,vendor_code,brand,sku2,barcode,category1,category2,category3,category4,sku_name,retail_price,cost_price,soh,open_po_total_qty,open_po_pending_receipt_qty,open_po_partially_received_qty,open_po_pending_cancellation_qty,first_grn_date,last_grn_date,payment_term_code,country,stock_entry_synched_at,sale_entry_synched_at,report_date,sold_qty_lifetime,sold_qty_yesterday,sold_qty_7days,sold_qty_14days,sold_qty_mtd,sold_qty_m1,sold_qty_m2,sold_qty_m3,sold_qty_m4,sold_qty_m5)
    SELECT @sku:=stock.sku, @location:=stock.location, @supplier:=stock.supplier, @supplier_item_no:=stock.supplier_item_no, @vendor_code:=stock.vendor_code, @brand:=COALESCE(stock.brand,sale.brand), @sku2:=stock.sku2,@barcode:=stock.barcode, @category1:=COALESCE(stock.category1,sale.category1),@category2:=COALESCE(stock.category2,sale.category2),@category3:=stock.category3,@category4:=stock.category4,@sku_name:=COALESCE(stock.sku_name,sale.sku_name),@retail_price:=stock.retail_price, @cost_price:=stock.cost_price,@soh:=stock.soh,@open_po_total_qty:=COALESCE(open_po.total_open_quantity,0),@open_po_pending_receipt_qty:=COALESCE(open_po.pending_receipt_quantity,0),@open_po_partially_received_qty:=COALESCE(open_po.partially_received_quantity,0),@open_po_pending_cancellation_qty:=COALESCE(open_po.pending_cancellation_quantity,0),@first_grn_date:=stock.first_grn_date, @last_grn_date:=stock.last_grn_date, @payment_term_code:=stock.payment_term_code,@country:=stock.country, @stock_entry_synched_at:=(stock.updated_at_utc+INTERVAL 3 HOUR), @sale_entry_synched_at:=sale.synched_at, @report_date:=DATE(NOW()+INTERVAL 3 HOUR), @sold_qty_lifetime:=sale.sold_qty_lifetime, @sold_qty_yesterday:=sale.sold_qty_yesterday, @sold_qty_7days:=sale.sold_qty_7days, @sold_qty_14days:=sale.sold_qty_14days,@sold_qty_mtd:=sale.sold_qty_mtd, @sold_qty_m1:=sale.sold_qty_m1,@sold_qty_m2:=sale.sold_qty_m2,@sold_qty_m3:=sale.sold_qty_m3, @sold_qty_m4:=sale.sold_qty_m4,@sold_qty_m5:=sale.sold_qty_m5
    FROM (select * from aoi.soh_entry_log where soh > 0) stock
    LEFT JOIN
    (select sku, sku_name, category1, category2, brand, SUM(IF(order_date <= yesterday, sold_qtys, 0)) sold_qty_lifetime,
    SUM(IF(order_date = yesterday, sold_qtys, 0)) sold_qty_yesterday,
    SUM(IF(order_date between (today - INTERVAL 7 DAY) AND yesterday, sold_qtys, 0)) sold_qty_7days,
    SUM(IF(order_date between (today - INTERVAL 14 DAY) AND yesterday, sold_qtys, 0)) sold_qty_14days,
    SUM(IF(order_date between mtd_first_day AND yesterday, sold_qtys, 0)) sold_qty_mtd,
    SUM(IF(order_date between m1_first_day AND m1_last_day, sold_qtys, 0)) sold_qty_m1,
    SUM(IF(order_date between m2_first_day AND m2_last_day, sold_qtys, 0)) sold_qty_m2,
    SUM(IF(order_date between m3_first_day AND m3_last_day, sold_qtys, 0)) sold_qty_m3,
    SUM(IF(order_date between m4_first_day AND m4_last_day, sold_qtys, 0)) sold_qty_m4,
    SUM(IF(order_date between m5_first_day AND m5_last_day, sold_qtys, 0)) sold_qty_m5, CONVERT_TZ(MAX(synched_at_utc),'+00:00','+03:00') synched_at FROM sku_sales,
    (select today, yesterday, DATE_ADD(DATE_ADD(mtd_last_day,INTERVAL 1 DAY),INTERVAL - 1 MONTH) mtd_first_day, mtd_last_day,
     DATE_ADD(DATE_ADD(m1_last_day,INTERVAL 1 DAY),INTERVAL - 1 MONTH) m1_first_day, m1_last_day,
     DATE_ADD(DATE_ADD(m2_last_day,INTERVAL 1 DAY),INTERVAL - 1 MONTH) m2_first_day, m2_last_day,
     DATE_ADD(DATE_ADD(m3_last_day,INTERVAL 1 DAY),INTERVAL - 1 MONTH) m3_first_day, m3_last_day,
     DATE_ADD(DATE_ADD(m4_last_day,INTERVAL 1 DAY),INTERVAL - 1 MONTH) m4_first_day, m4_last_day,
     DATE_ADD(DATE_ADD(m5_last_day,INTERVAL 1 DAY),INTERVAL - 1 MONTH) m5_first_day, m5_last_day
     FROM (select CONVERT_TZ(now(),'+00:00','+03:00') time_now, date(CONVERT_TZ(now(),'+00:00','+03:00')) today, date(CONVERT_TZ(now(),'+00:00','+03:00') - INTERVAL 1 DAY) yesterday, LAST_DAY(CONVERT_TZ(now(),'+00:00','+03:00')) mtd_last_day, LAST_DAY(LAST_DAY(CONVERT_TZ(now(),'+00:00','+03:00')) - INTERVAL 1 MONTH) m1_last_day, LAST_DAY(LAST_DAY(CONVERT_TZ(now(),'+00:00','+03:00')) - INTERVAL 2 MONTH) m2_last_day, LAST_DAY(LAST_DAY(CONVERT_TZ(now(),'+00:00','+03:00')) - INTERVAL 3 MONTH) m3_last_day, LAST_DAY(LAST_DAY(CONVERT_TZ(now(),'+00:00','+03:00')) - INTERVAL 4 MONTH) m4_last_day, LAST_DAY(LAST_DAY(CONVERT_TZ(now(),'+00:00','+03:00')) - INTERVAL 5 MONTH) m5_last_day) sale_window) sale_window
     group by sku having sold_qty_lifetime > 0) sale ON stock.sku=sale.sku
    LEFT JOIN (SELECT sku, SUM(COALESCE(total_open_quantity,0))total_open_quantity, SUM(COALESCE(pending_receipt_quantity,0)) pending_receipt_quantity, SUM(COALESCE(partially_received_quantity,0)) partially_received_quantity, SUM(COALESCE(pending_cancellation_quantity,0)) pending_cancellation_quantity FROM aoi.open_po GROUP BY sku having total_open_quantity > 0) open_po ON open_po.sku = stock.sku
    """
    ofs_conn.execute(upsert_soh_report_query)
    #time.sleep(60)
    extra_upsert_soh_report_query="""
    REPLACE INTO aoi.soh_report (sku,location,supplier,supplier_item_no,vendor_code,brand,sku2,barcode,category1,category2,category3,category4,sku_name,retail_price,cost_price,soh,open_po_total_qty,open_po_pending_receipt_qty,open_po_partially_received_qty,open_po_pending_cancellation_qty,first_grn_date,last_grn_date,payment_term_code,country,stock_entry_synched_at,sale_entry_synched_at,report_date,sold_qty_lifetime,sold_qty_yesterday,sold_qty_7days,sold_qty_14days,sold_qty_mtd,sold_qty_m1,sold_qty_m2,sold_qty_m3,sold_qty_m4,sold_qty_m5)
    select sale_open_po_full.sku, sku_details.location, sku_details.supplier,sku_details.supplier_item_no,sku_details.vendor_code, COALESCE(sale_open_po_full.brand,sku_details.brand), sku_details.sku2,sku_details.barcode, COALESCE(sale_open_po_full.category1,sku_details.category1), COALESCE(sale_open_po_full.category2,sku_details.category2), sku_details.category3,sku_details.category4,COALESCE(sale_open_po_full.sku_name,sku_details.sku_name),sku_details.retail_price,sku_details.cost_price,sku_details.soh,
    sale_open_po_full.open_po_total_qty,sale_open_po_full.open_po_pending_receipt_qty,sale_open_po_full.open_po_partially_received_qty,sale_open_po_full.open_po_pending_cancellation_qty,sku_details.first_grn_date,sku_details.last_grn_date,sku_details.payment_term_code,sku_details.country,(sku_details.updated_at_utc+INTERVAL 3 HOUR)stock_entry_synched_at,
    sale_open_po_full.sale_entry_synched_at, sale_open_po_full.report_date, sale_open_po_full.sold_qty_lifetime, sale_open_po_full.sold_qty_yesterday, sale_open_po_full.sold_qty_7days, sale_open_po_full.sold_qty_14days, sale_open_po_full.sold_qty_mtd, sale_open_po_full.sold_qty_m1, sale_open_po_full.sold_qty_m2, sale_open_po_full.sold_qty_m3, sale_open_po_full.sold_qty_m4, sale_open_po_full.sold_qty_m5 FROM 
    (select sale.sku, sale.brand, sale.category1, sale.category2,sale.sku_name, sale.synched_at sale_entry_synched_at, DATE(NOW()+INTERVAL 3 HOUR) report_date, sale.sold_qty_lifetime, sale.sold_qty_yesterday, sale.sold_qty_7days, sale.sold_qty_14days,sale.sold_qty_mtd, sale.sold_qty_m1,sale.sold_qty_m2,sale.sold_qty_m3, sale.sold_qty_m4,sale.sold_qty_m5,
    COALESCE(open_po.total_open_quantity,0) open_po_total_qty, COALESCE(open_po.pending_receipt_quantity,0) open_po_pending_receipt_qty, COALESCE(open_po.partially_received_quantity,0) open_po_partially_received_qty, COALESCE(open_po.pending_cancellation_quantity,0) open_po_pending_cancellation_qty FROM
    (select sku, sku_name, category1, category2, brand, SUM(IF(order_date <= yesterday, sold_qtys, 0)) sold_qty_lifetime,
    SUM(IF(order_date = yesterday, sold_qtys, 0)) sold_qty_yesterday,
    SUM(IF(order_date between (today - INTERVAL 7 DAY) AND yesterday, sold_qtys, 0)) sold_qty_7days,
    SUM(IF(order_date between (today - INTERVAL 14 DAY) AND yesterday, sold_qtys, 0)) sold_qty_14days,
    SUM(IF(order_date between mtd_first_day AND yesterday, sold_qtys, 0)) sold_qty_mtd,
    SUM(IF(order_date between m1_first_day AND m1_last_day, sold_qtys, 0)) sold_qty_m1,
    SUM(IF(order_date between m2_first_day AND m2_last_day, sold_qtys, 0)) sold_qty_m2,
    SUM(IF(order_date between m3_first_day AND m3_last_day, sold_qtys, 0)) sold_qty_m3,
    SUM(IF(order_date between m4_first_day AND m4_last_day, sold_qtys, 0)) sold_qty_m4,
    SUM(IF(order_date between m5_first_day AND m5_last_day, sold_qtys, 0)) sold_qty_m5, CONVERT_TZ(MAX(synched_at_utc),'+00:00','+03:00') synched_at FROM sku_sales,
    (select today, yesterday, DATE_ADD(DATE_ADD(mtd_last_day,INTERVAL 1 DAY),INTERVAL - 1 MONTH) mtd_first_day, mtd_last_day,
     DATE_ADD(DATE_ADD(m1_last_day,INTERVAL 1 DAY),INTERVAL - 1 MONTH) m1_first_day, m1_last_day,
     DATE_ADD(DATE_ADD(m2_last_day,INTERVAL 1 DAY),INTERVAL - 1 MONTH) m2_first_day, m2_last_day,
     DATE_ADD(DATE_ADD(m3_last_day,INTERVAL 1 DAY),INTERVAL - 1 MONTH) m3_first_day, m3_last_day,
     DATE_ADD(DATE_ADD(m4_last_day,INTERVAL 1 DAY),INTERVAL - 1 MONTH) m4_first_day, m4_last_day,
     DATE_ADD(DATE_ADD(m5_last_day,INTERVAL 1 DAY),INTERVAL - 1 MONTH) m5_first_day, m5_last_day
     FROM (select CONVERT_TZ(now(),'+00:00','+03:00') time_now, date(CONVERT_TZ(now(),'+00:00','+03:00')) today, date(CONVERT_TZ(now(),'+00:00','+03:00') - INTERVAL 1 DAY) yesterday, LAST_DAY(CONVERT_TZ(now(),'+00:00','+03:00')) mtd_last_day, LAST_DAY(LAST_DAY(CONVERT_TZ(now(),'+00:00','+03:00')) - INTERVAL 1 MONTH) m1_last_day, LAST_DAY(LAST_DAY(CONVERT_TZ(now(),'+00:00','+03:00')) - INTERVAL 2 MONTH) m2_last_day, LAST_DAY(LAST_DAY(CONVERT_TZ(now(),'+00:00','+03:00')) - INTERVAL 3 MONTH) m3_last_day, LAST_DAY(LAST_DAY(CONVERT_TZ(now(),'+00:00','+03:00')) - INTERVAL 4 MONTH) m4_last_day, LAST_DAY(LAST_DAY(CONVERT_TZ(now(),'+00:00','+03:00')) - INTERVAL 5 MONTH) m5_last_day) sale_window) sale_window
     group by sku having sold_qty_lifetime > 0) sale LEFT JOIN (SELECT sku, SUM(COALESCE(total_open_quantity,0))total_open_quantity, SUM(COALESCE(pending_receipt_quantity,0)) pending_receipt_quantity, SUM(COALESCE(partially_received_quantity,0)) partially_received_quantity, SUM(COALESCE(pending_cancellation_quantity,0)) pending_cancellation_quantity FROM aoi.open_po GROUP BY sku having total_open_quantity > 0) open_po ON sale.sku = open_po.sku
     UNION ALL
     select open_po.sku, sale.brand, sale.category1, sale.category2,sale.sku_name, sale.synched_at sale_entry_synched_at, DATE(NOW()+INTERVAL 3 HOUR) report_date, sale.sold_qty_lifetime, sale.sold_qty_yesterday, sale.sold_qty_7days, sale.sold_qty_14days,sale.sold_qty_mtd, sale.sold_qty_m1,sale.sold_qty_m2,sale.sold_qty_m3, sale.sold_qty_m4,sale.sold_qty_m5,
    COALESCE(open_po.total_open_quantity,0) open_po_total_qty, COALESCE(open_po.pending_receipt_quantity,0) open_po_pending_receipt_qty, COALESCE(open_po.partially_received_quantity,0) open_po_partially_received_qty, COALESCE(open_po.pending_cancellation_quantity,0) open_po_pending_cancellation_qty
     FROM (select sku, sku_name, category1, category2, brand, SUM(IF(order_date <= yesterday, sold_qtys, 0)) sold_qty_lifetime, 
     SUM(IF(order_date = yesterday, sold_qtys, 0)) sold_qty_yesterday,
     SUM(IF(order_date between (today - INTERVAL 7 DAY) AND yesterday, sold_qtys, 0)) sold_qty_7days,
     SUM(IF(order_date between (today - INTERVAL 14 DAY) AND yesterday, sold_qtys, 0)) sold_qty_14days,
     SUM(IF(order_date between mtd_first_day AND yesterday, sold_qtys, 0)) sold_qty_mtd,
     SUM(IF(order_date between m1_first_day AND m1_last_day, sold_qtys, 0)) sold_qty_m1,
     SUM(IF(order_date between m2_first_day AND m2_last_day, sold_qtys, 0)) sold_qty_m2,
    SUM(IF(order_date between m3_first_day AND m3_last_day, sold_qtys, 0)) sold_qty_m3,
    SUM(IF(order_date between m4_first_day AND m4_last_day, sold_qtys, 0)) sold_qty_m4,
    SUM(IF(order_date between m5_first_day AND m5_last_day, sold_qtys, 0)) sold_qty_m5, CONVERT_TZ(MAX(synched_at_utc),'+00:00','+03:00') synched_at FROM sku_sales,
    (select today, yesterday, DATE_ADD(DATE_ADD(mtd_last_day,INTERVAL 1 DAY),INTERVAL - 1 MONTH) mtd_first_day, mtd_last_day,
     DATE_ADD(DATE_ADD(m1_last_day,INTERVAL 1 DAY),INTERVAL - 1 MONTH) m1_first_day, m1_last_day,
     DATE_ADD(DATE_ADD(m2_last_day,INTERVAL 1 DAY),INTERVAL - 1 MONTH) m2_first_day, m2_last_day,
     DATE_ADD(DATE_ADD(m3_last_day,INTERVAL 1 DAY),INTERVAL - 1 MONTH) m3_first_day, m3_last_day,
     DATE_ADD(DATE_ADD(m4_last_day,INTERVAL 1 DAY),INTERVAL - 1 MONTH) m4_first_day, m4_last_day,
     DATE_ADD(DATE_ADD(m5_last_day,INTERVAL 1 DAY),INTERVAL - 1 MONTH) m5_first_day, m5_last_day
     FROM (select CONVERT_TZ(now(),'+00:00','+03:00') time_now, date(CONVERT_TZ(now(),'+00:00','+03:00')) today, date(CONVERT_TZ(now(),'+00:00','+03:00') - INTERVAL 1 DAY) yesterday, LAST_DAY(CONVERT_TZ(now(),'+00:00','+03:00')) mtd_last_day, LAST_DAY(LAST_DAY(CONVERT_TZ(now(),'+00:00','+03:00')) - INTERVAL 1 MONTH) m1_last_day, LAST_DAY(LAST_DAY(CONVERT_TZ(now(),'+00:00','+03:00')) - INTERVAL 2 MONTH) m2_last_day, LAST_DAY(LAST_DAY(CONVERT_TZ(now(),'+00:00','+03:00')) - INTERVAL 3 MONTH) m3_last_day, LAST_DAY(LAST_DAY(CONVERT_TZ(now(),'+00:00','+03:00')) - INTERVAL 4 MONTH) m4_last_day, LAST_DAY(LAST_DAY(CONVERT_TZ(now(),'+00:00','+03:00')) - INTERVAL 5 MONTH) m5_last_day) sale_window) sale_window
     group by sku having sold_qty_lifetime > 0) sale RIGHT JOIN (SELECT sku, SUM(COALESCE(total_open_quantity,0))total_open_quantity, SUM(COALESCE(pending_receipt_quantity,0)) pending_receipt_quantity, SUM(COALESCE(partially_received_quantity,0)) partially_received_quantity, SUM(COALESCE(pending_cancellation_quantity,0)) pending_cancellation_quantity FROM aoi.open_po GROUP BY sku having total_open_quantity > 0) open_po ON sale.sku = open_po.sku
     WHERE sale.sku IS NULL) sale_open_po_full
     LEFT JOIN aoi.soh_entry_log sku_details ON sku_details.sku = sale_open_po_full.sku
     LEFT JOIN aoi.soh_report ON sale_open_po_full.sku = soh_report.sku 
     WHERE soh_report.sku IS NULL
    """
    ofs_conn.execute(extra_upsert_soh_report_query)
except:
    raise Exception("Couldn't upsert data into soh_report table!")

#time.sleep(30)
try:
    ofs_conn.execute("UPDATE aoi.soh_report join (select sku, sum(coalesce(quantity,0)) reserved_qty from aoi.ofs_pending_pick_items group by sku) ofs_reserved ON soh_report.sku=ofs_reserved.sku SET soh_report.reserved_qty=ofs_reserved.reserved_qty")
except:
    raise Exception("Couldn't update reserved_qty in SOH report from OFS items under processing")

try:
    ofs_conn.execute("update aoi.soh_report stock JOIN (SELECT sku, SUM(COALESCE(total_open_quantity,0))total_open_quantity, SUM(COALESCE(pending_receipt_quantity,0)) pending_receipt_quantity, SUM(COALESCE(partially_received_quantity,0)) partially_received_quantity, SUM(COALESCE(pending_cancellation_quantity,0)) pending_cancellation_quantity FROM aoi.open_po GROUP BY sku having total_open_quantity > 0) open_po ON open_po.sku = stock.sku SET stock.open_po_total_qty=open_po.total_open_quantity, stock.open_po_pending_receipt_qty=open_po.pending_receipt_quantity, stock.open_po_partially_received_qty=open_po.partially_received_quantity, stock.open_po_pending_cancellation_qty=open_po.pending_cancellation_quantity")
except:
    raise Exception("Couldn't bulk update open_po_qty in SOH report from open_po table")

try:
    ofs_conn.execute("update aoi.soh_report s join aoi.unisoft_sku_grn_dates u on s.sku=u.sku set s.first_grn_date=IF(s.first_grn_date IS NULL OR s.first_grn_date > u.first_grn_date, u.first_grn_date, s.first_grn_date)")
    ofs_conn.execute("update aoi.soh_report s join aoi.unisoft_sku_grn_dates u on s.sku=u.sku set s.last_grn_date = u.last_grn_date where s.last_grn_date IS NULL")
except:
    raise Exception("Couldn't update first_grn & last_grn in SOH report from unisoft_sku_grn_dates table")

try:
    ofs_conn.execute("DELETE FROM aoi.soh_report WHERE report_date <> DATE(NOW()+INTERVAL 3 HOUR)")
    ofs_conn.execute("UPDATE aoi.soh_report set sellable_quantity=0, not_sellable_quantity=0, nav_total_quantity=0")
    ofs_conn.execute("UPDATE aoi.soh_report sr JOIN aoi.consolidated_sku_stock css on sr.sku=css.sku SET sr.sellable_quantity=COALESCE(css.nav_warehouse_sellable,0), sr.not_sellable_quantity=COALESCE(css.nav_warehouse_not_sellable,0)+COALESCE(css.nav_others_not_sellable,0), sr.nav_total_quantity=COALESCE(css.nav_total,0)")
except:
    raise Exception("Couldn't clean outdated soh_report entries")



