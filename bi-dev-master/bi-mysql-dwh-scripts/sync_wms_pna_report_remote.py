#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu Apr 18 10:58:29 2019

@author: muthu
"""
from sqlalchemy import create_engine
wms_conn = create_engine("mssql+pymssql://usr1:pass1@0.0.0.1:1455/db1")
try:
    wms_conn.execution_options(autocommit=True).execute("TRUNCATE TABLE [BI].[DBO].[WMS_PNA_CONSOLIDATE]")
except:
    raise Exception("Couldn't empty the WMS_PNA_CONSOLIDATE table!")

try:
    wms_conn.execute("INSERT INTO [BI].[DBO].[WMS_PNA_CONSOLIDATE](ITEMID,SKU,SKU_NAME,TRANSDATE,PNA,RowNum) SELECT ITEMID,SKU,SKU_NAME,TRANSDATE,PNA,row_number() over(partition by ITEMID order by TRANSDATE desc) as RowNum FROM (SELECT ITEMID,ITEMNO AS SKU,DESCRIPTION AS SKU_NAME,CONVERT(DATE,TRANSDATE) AS TRANSDATE,PNA FROM [NAVWMS].[DBO].[WMS_PICKING] WHERE PNA=1 UNION ALL SELECT ITEMID,ITEMNO AS SKU,DESCRIPTION AS SKU_NAME,CONVERT(DATE,TRANSDATE) AS TRANSDATE,PNA FROM [NAVWMS].[DBO].[ARC_PICKING] WHERE PNA=1 UNION ALL SELECT ITEMID,ITEMNO AS SKU,DESCRIPTION AS SKU_NAME,CONVERT(DATE,TRANSDATE) AS TRANSDATE,PNA FROM [NAVWMS].[DBO].[WMS_PNA_PICKING] UNION ALL SELECT ITEMID,ITEMNO AS SKU,DESCRIPTION AS SKU_NAME,CONVERT(DATE,TRANSDATE) AS TRANSDATE,PNA FROM [NAVWMS].[DBO].[ARC_PNA_PICKING])PNA_CONSOLIDATE")
except:
    raise Exception("Couldn't load data into WMS_PNA_CONSOLIDATE table!")

ResultProxy = wms_conn.execute("SELECT * FROM [BI].[DBO].[WMS_PNA_REPORT]")
pna_list = ResultProxy.fetchall()
chunk_size = 10

ofs_conn = create_engine("mysql+pymysql://usr2:pass2@0.0.0.2/db2")
try:
    ofs_conn.execute("TRUNCATE TABLE aoi.wms_pna_report_log")
except:
    raise Exception("Couldn't empty the wms_pna_report_log table!")

for i in range(0, len(pna_list), chunk_size):
    pna_list_chunk = pna_list[i:i+chunk_size]
    insert_query="INSERT IGNORE INTO aoi.wms_pna_report_log(item_id, sku, sku_name, n_pna_marked, first_pna_date, recent_status, recent_status_date) values "
    insert_query = insert_query + str(pna_list_chunk[0])
    for row in pna_list_chunk[1:]:
        insert_query = insert_query + ', ' + str(row)

    try:
        ofs_conn.execute(insert_query)
        print("Inserted "+ str(i+chunk_size) + " rows")
    except:
        raise Exception("Couldn't load data into recent wms_pna_report_log table!")

try:
    ofs_conn.execute("insert into aoi.wms_pna_report (item_id, sku, sku_name, n_pna_marked, first_pna_date, recent_status, recent_status_date, order_date, order_number, reserved_stock, available_stock, blocked_pigeon_hole_code, synched_at) select @item_id := pna_items.item_id, @sku := pna_items.sku, @sku_name := pna_items.sku_name, @n_pna_marked := pna_items.n_pna_marked, @first_pna_date := pna_items.first_pna_date, @recent_status := pna_items.recent_status, @recent_status_date := pna_items.recent_status_date, @order_date := pna_items.order_date, @order_number := pna_items.order_number, @reserved_stock := pna_items.reserved, @available_stock := pna_items.available, @blocked_pigeon_hole_code := bin_info.BinCode, @synched_at := (NOW()+INTERVAL 3 HOUR) from (select pna_report.*, order_date, order_number, reserved, available from aoi.wms_pna_report_log pna_report left join aoi.order_details items on items.item_id = pna_report.item_id left join aoi.sku_stock crs_stock on crs_stock.sku = pna_report.sku group by pna_report.item_id) pna_items left join OFS.SortingBinDetails bin_info on bin_info.WebOrderNo = pna_items.order_number group by pna_items.item_id ON DUPLICATE KEY UPDATE sku = @sku, sku_name = @sku_name, n_pna_marked = @n_pna_marked, first_pna_date = @first_pna_date, recent_status = @recent_status, recent_status_date = @recent_status_date, order_date = @order_date, order_number = @order_number, reserved_stock = @reserved_stock, available_stock = @available_stock, blocked_pigeon_hole_code = @blocked_pigeon_hole_code, synched_at = @synched_at")
except:
    raise Exception("Couldn't upsert data into wms_pna_report table!")

