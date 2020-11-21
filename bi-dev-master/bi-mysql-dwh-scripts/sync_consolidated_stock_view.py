#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon Jun 17 04:11:43 2019

@author: muthu
"""
import requests
import json
import dateutil.parser
from sqlalchemy import create_engine

nav_conn = create_engine("mssql+pymssql://usr1:pass1@0.0.0.1:1455/db1")
ofs_conn = create_engine("mysql+pymysql://usr2:pass2@0.0.0.2/db2")
crs_stockallsku_api = 'http://feed.boutiqaat.net/api/v1/stockall'

def sync_crs_stock():
    try:
        ofs_conn.execute("TRUNCATE TABLE aoi.crs_stock")
    except:
        raise Exception("Couldn't empty the CRS stock table!")

    sku_list_resp = requests.get(crs_stockallsku_api)
    sku_data = json.loads(sku_list_resp.text)
    stock_data_arr = sku_data["data"]["payload"]
    stock_info = stock_data_arr[0]
    sku = stock_info["sku"]
    available = str(stock_info["a"])
    reserved = str(stock_info["r"])
    total = str(stock_info["t"])
    updated_at = str(stock_info["uAt"])
    updated_at = dateutil.parser.parse(updated_at)
    updated_at = updated_at.strftime("%Y-%m-%d %H:%M:%S")
    query_values = "('" + sku + "'," + available + "," + reserved + "," + total + ",'" +  updated_at + "')"
    for stock_info in stock_data_arr[1:]:
        sku = stock_info["sku"]
        available = str(stock_info["a"])
        reserved = str(stock_info["r"])
        total = str(stock_info["t"])
        updated_at = stock_info["uAt"]
        updated_at = dateutil.parser.parse(updated_at)
        updated_at = updated_at.strftime("%Y-%m-%d %H:%M:%S")
        query_values = query_values + ",('" + sku + "'," + available + "," + reserved + "," + total + ",'" + updated_at + "')"

    insert_stock_entry_query = "INSERT INTO aoi.crs_stock(sku,available,reserved,total,updated_at) values "+query_values
    try:
        #print(insert_stock_entry_query)
        ofs_conn.execute(insert_stock_entry_query)
        ofs_conn.execute("TRUNCATE TABLE aoi.ofs_sku_reserved")
        ofs_conn.execute("INSERT INTO aoi.ofs_sku_reserved select sku, sum(coalesce(quantity,0)) ofs_reserved from aoi.ofs_pending_pick_items group by sku")
    except:
        raise Exception("Couldn't bulk insert CRS stock details!")

sync_crs_stock()
try:
    sellable_location_stock_select_query = """
        select [Location Code] location, [Bin Type Code] bin_type_code, [Bin Code] bin_code, [Item No_] as sku, 
        CONVERT(DECIMAL(10,3),SUM(COALESCE([Quantity],0))) quantity, 
        CONVERT(DATETIME2(0),MIN([Insert Date Time]))  min_insert_time,
        CONVERT(DATETIME2(0),MAX([Insert Date Time]))  max_insert_time 
        from [dbo].[Boutiqaat Kuwait$Warehouse Entry] WITH (NOLOCK)
        Inner Join (select [Boutiqaat Kuwait$Location].[Code] as location
        from [dbo].[Boutiqaat Kuwait$Location] WITH (NOLOCK) where [Boutiqaat Kuwait$Location].[Bin Mandatory]=1) as sellable_location ON sellable_location.location=[Location Code]
        GROUP BY [Location Code], [Bin Type Code], [Bin Code], [Item No_]
    """
    ResultProxy = nav_conn.execute(sellable_location_stock_select_query)
    sellable_stock_records = ResultProxy.fetchall()
    try:
        ofs_conn.execute("TRUNCATE TABLE aoi.nav_sellable_location_stock")
    except:
        raise Exception("Couldn't empty aoi.nav_sellable_location_stock table for the next refresh")

    chunk_size = 100
    for i in range(0, len(sellable_stock_records), chunk_size):
        sellable_stock_values = sellable_stock_records[i:i+chunk_size]
        insert_query="INSERT INTO aoi.nav_sellable_location_stock(location, bin_type_code, bin_code, sku, quantity, min_insert_time, max_insert_time) values"
        insert_query = insert_query + str(sellable_stock_values[0]).replace("%","%%").replace("Decimal('","").replace("')","").replace("None","NULL") + "')"
        for value_row in sellable_stock_values[1:]:
            insert_query = insert_query + ', ' + str(value_row).replace("%","%%").replace("Decimal('","").replace("')","").replace("None","NULL") + "')"

        try:
            ofs_conn.execute(insert_query)
            print("Inserted "+ str(i+chunk_size) + " rows into nav_sellable_location_stock table")
        except:
            raise Exception("Couldn't load data into recent nav_sellable_location_stock table!")


    not_sellable_location_stock_select_query = """
        SELECT [Location Code] location, [Item No_] sku, CONVERT(DATETIME2(0),MIN([Posting Date]))  min_posting_time, 
        CONVERT(DATETIME2(0),MAX([Posting Date]))  max_posting_time, CONVERT(DECIMAL(10,3),SUM(COALESCE([Quantity],0))) quantity
        from Boutiqaat_Live.dbo.[Boutiqaat Kuwait$Item Ledger Entry] WITH (NOLOCK)
        Inner Join (select [Boutiqaat Kuwait$Location].[Code] as location from Boutiqaat_Live.dbo.[Boutiqaat Kuwait$Location] WITH (NOLOCK) WHERE [Boutiqaat Kuwait$Location].[Bin Mandatory]=0)unsellable_location ON unsellable_location.location=[Location Code]
        GROUP BY [Location Code],[Item No_] having SUM(COALESCE([Quantity],0)) > 0
    """
    ResultProxy = nav_conn.execute(not_sellable_location_stock_select_query)
    not_sellable_stock_records = ResultProxy.fetchall()
    try:
        ofs_conn.execute("TRUNCATE TABLE aoi.nav_notsellable_location_stock")
    except:
        raise Exception("Couldn't empty nav_notsellable_location_stock table for the next refresh")

    chunk_size = 100
    for i in range(0, len(not_sellable_stock_records), chunk_size):
        not_sellable_stock_values = not_sellable_stock_records[i:i+chunk_size]
        insert_query="INSERT INTO aoi.nav_notsellable_location_stock(location, sku, min_insert_time, max_insert_time, quantity) values"
        insert_query = insert_query + str(not_sellable_stock_values[0]).replace("%","%%").replace("Decimal('","").replace("')","").replace("None","NULL")
        for value_row in not_sellable_stock_values[1:]:
            insert_query = insert_query + ', ' + str(value_row).replace("%","%%").replace("Decimal('","").replace("')","").replace("None","NULL")

        try:
            ofs_conn.execute(insert_query)
            print("Inserted "+ str(i+chunk_size) + " rows into nav_notsellable_location_stock table")
        except:
            raise Exception("Couldn't load data into recent nav_notsellable_location_stock table!")


    nav_stock_detail_select_query = """
        SELECT sd.[Item No] sku, CONVERT(DATETIME2(0),MaxEntryAt)  max_insert_at, [Qty in Stock] nav2crs_total, [Warehouse Entry] wh_entry, 
        [Warehouse JN] wh_jn_line,    [Warehouse Activity Line] wh_activity_line,    [Warehouse Activity Line 2] wh_activity_line2,
        [Movement Journal Line] move_jn_line FROM (SELECT [Item No] SKU, Max([Insert At]) as MaxEntryAt 
        FROM [Boutiqaat Kuwait$Stock Details] WITH (NOLOCK) GROUP BY [Item No]) sd_last_entry 
        INNER JOIN [Boutiqaat Kuwait$Stock Details] sd WITH (NOLOCK) ON sd.[Item No] = sd_last_entry.SKU AND 
        sd.[Insert At] = sd_last_entry.MaxEntryAt
    """
    ResultProxy = nav_conn.execute(nav_stock_detail_select_query)
    nav_stock_detail_records = ResultProxy.fetchall()
    try:
        ofs_conn.execute("TRUNCATE TABLE aoi.nav_stock_details_entry")
    except:
        raise Exception("Couldn't empty nav_stock_details_entry table for the next refresh")

    chunk_size = 100
    for i in range(0, len(nav_stock_detail_records), chunk_size):
        nav_stock_detail_values = nav_stock_detail_records[i:i+chunk_size]
        insert_query="INSERT INTO aoi.nav_stock_details_entry(sku, max_insert_at, nav2crs_total, wh_entry, wh_jn_line, wh_activity_line, wh_activity_line2, move_jn_line) values"
        insert_query = insert_query + str(nav_stock_detail_values[0]).replace("%","%%").replace("Decimal('","").replace("')","").replace("None","NULL")
        for value_row in nav_stock_detail_values[1:]:
            insert_query = insert_query + ', ' + str(value_row).replace("%","%%").replace("Decimal('","").replace("')","").replace("None","NULL")

        try:
            ofs_conn.execute(insert_query)
            print("Inserted "+ str(i+chunk_size) + " rows into nav_stock_details_entry table")
        except:
            raise Exception("Couldn't load data into recent nav_stock_details_entry table!")


except:
    raise Exception("Couldn't query SOH data from NAV DB!")

consolidated_sku_stock_insert_query="""
replace into aoi.consolidated_sku_stock select sku_stock.*,
(coalesce(nav_warehouse_sellable,0)+coalesce(nav_warehouse_not_sellable,0)+coalesce(nav_others_not_sellable,0)) nav_total, 
(coalesce(crs_available,0) - coalesce(nav_warehouse_sellable,0) + coalesce(ofs_not_picked_or_cancelled,0)) crs_available_diff, 
(coalesce(crs_reserved,0) - coalesce(ofs_not_picked_or_cancelled,0) ) crs_reserved_diff, 
(coalesce(nav_warehouse_sellable,0)+coalesce(nav_warehouse_not_sellable,0)+coalesce(nav_others_not_sellable,0)-coalesce(ofs_not_picked_or_cancelled,0)) soh  
from (select crs_stock.sku, (crs_stock.synched_at_utc+INTERVAL 3 HOUR) report_at_AST, 
SUM(coalesce(sde.nav2crs_total,0)) nav_warehouse_sellable,
SUM(coalesce(sde.wh_entry-sde.nav2crs_total,0)+nav_wh_entry.not_sellable) nav_warehouse_not_sellable, 
SUM(nav_others.not_sellable) nav_others_not_sellable,  
SUM(coalesce(sde.nav2crs_total,0)) nav2crs_total, 
SUM(coalesce(crs_stock.reserved,0)) crs_reserved, 
SUM(coalesce(crs_stock.available,0)) crs_available, SUM(coalesce(crs_stock.total,0)) crs_total, 
SUM(coalesce(ofs_reserved,0)) ofs_not_picked_or_cancelled
from crs_stock
left join nav_stock_details_entry sde on crs_stock.sku=sde.sku
left join (select sku, SUM((CASE WHEN bin_type_code='RECEIVE' THEN coalesce(quantity,0) ELSE 0 END)+(CASE WHEN bin_type_code='QC' AND bin_code<> 'STAGEMOVEBIN' THEN coalesce(quantity,0) ELSE 0 END)) not_sellable FROM nav_sellable_location_stock GROUP BY sku) nav_wh_entry on crs_stock.sku=nav_wh_entry.sku
left join (select sku, SUM(coalesce(quantity,0)) not_sellable FROM nav_notsellable_location_stock GROUP BY sku) nav_others on crs_stock.sku=nav_others.sku
left join ofs_sku_reserved on crs_stock.sku=ofs_sku_reserved.sku
group by crs_stock.sku) sku_stock
"""
try:
    ofs_conn.execute(consolidated_sku_stock_insert_query)
    ofs_conn.execute("INSERT IGNORE INTO aoi.consolidated_sku_stock_filter SELECT sku,nav2crs_total FROM aoi.consolidated_sku_stock")
    print("Inserted "+ str(i+chunk_size) + " rows into aoi.consolidated_sku_stock table")
except:
    raise Exception("Couldn't load data into aoi.consolidated_sku_stock table!")


