SHELL := /bin/bash
S3CMD=aws s3

current_month = $(shell date +"%m")
current_year = $(shell date +"%Y")

daily: sku_live_status_report brand_performance in_stock_remark_report mkt_roi_automation global_score_report_daily 7days_attr_report sale_traffic_ranking customer_retention firebase_page_loaded_ctr_summary instock_disabled_sku mkt_order_summary aging_sell_through_table

hourly: nav_item_sku_master order_details aoi_update_sku_sales aoi_sku_not_sellable nav_sku_stock_location purch_hourly sales_stats_per_hour nav_item_sales_price_history nav_not_sellable_items_qty nav_total_items_qty ofs_sales_orders nav_soh_entry_log

#orders_hourly: nav_item_sku_master order_details aoi_update_sku_sales aoi_soh_report aoi_sku_not_sellable nav_sku_stock_location purch_hourly sales_stats_per_hour

nav_item_sales_price_history:
	cat queries/aoi/nav_item_sales_price_history.sql | redshift

nav_not_sellable_items_qty:
	cat queries/aoi/nav_not_sellable_items_qty.sql | redshift

nav_total_items_qty:
	cat queries/aoi/nav_total_items_qty.sql | redshift

nav_soh_entry_log:
	cat queries/aoi/nav_soh_entry_log.sql | redshift

nav_open_po:
	cat queries/aoi/nav_open_po.sql | redshift

nav_item_sku_master:
	cat queries/aoi/nav_item_sku_master.sql | redshift

ofs_sales_orders:
	cat queries/aoi/ofs_sales_orders.sql | redshift

order_details: aoi_order_details aoi_update_order_details

aoi_order_details:
	cat queries/aoi/aoi_order_details.sql | redshift

aoi_update_order_details:
	cat queries/aoi/aoi_update_order_details.sql | redshift

aoi_update_sku_sales:
	cat queries/aoi/aoi_update_sku_sales.sql | redshift

# aoi_soh_report:
# 	cat queries/aoi/aoi_soh_report.sql | redshift

aoi_sku_not_sellable:
	cat queries/aoi/aoi_sku_not_sellable.sql | redshift


#######finance reports######

finance_daily: stock_balance_by_store_loc supplier_stock_movement

stock_balance_by_store_loc:
	cat queries/finance/stock_balance_by_store_loc.sql | redshift

supplier_stock_movement:
	cat queries/finance/supplier_stock_movement.sql | redshift


####### Analytics ######

purch_hourly: mk_sales_order_items sales_stats_per_hour sales_stats_monthly_weekly magento_celeb_prod parent_child_sku_mapping

mk_sales_order_items:
	cat queries/analytics/mk_sales_order_items.sql | redshift

sales_stats_per_hour:
	cat queries/analytics/sales_stats_per_hour.sql | redshift

sales_stats_monthly_weekly:
	cat queries/analytics/sales_stats_monthly_weekly.sql | redshift

magento_celeb_prod:
	cat queries/analytics/magento_celeb_prod.sql | redshift

parent_child_sku_mapping:
	cat queries/analytics/parent_child_sku_mapping.sql | redshift

brand_performance:
	cat queries/analytics/brand_performance.sql | redshift

in_stock_remark_report:
	cat queries/analytics/in_stock_remark_report.sql | redshift

# inventory_health:
# 	cat queries/analytics/inventory_health.sql | redshift

#sandbox_daily: sku_live_status_report mkt_roi_automation

sku_live_status_report:
	cat queries/analytics/sku_live_status_report.sql | redshift

nav_sku_stock_location:
	cat queries/analytics/nav_sku_stock_location.sql | redshift

mkt_roi_automation:
	cat queries/analytics/mkt_roi_automation.sql | redshift

global_score_report_daily:
	cat queries/analytics/global_score_report_daily.sql | redshift

7days_attr_report:	
	cat queries/analytics/7days_attr_report.sql | redshift

sale_traffic_ranking:	
	cat queries/analytics/sale_traffic_ranking.sql | redshift

soh_report:	
	cat queries/analytics/soh_report.sql | redshift

emt_events_report_ext:	
	cat queries/analytics/emt_events_report_ext.sql | redshift
	
####### Sandbox ######
customer_retention:	
	cat queries/sandbox/customer_retention.sql | redshift

firebase_page_loaded_ctr_summary:
	cat queries/sandbox/firebase_page_loaded_ctr_summary.sql | redshift
	
instock_disabled_sku:	
	cat queries/sandbox/instock_disabled_sku.sql | redshift	

mkt_order_summary:	
	cat queries/sandbox/mkt_order_summary.sql | redshift
	
aging_sell_through_table:	
	cat queries/sandbox/aging_sell_through_table.sql | redshift



####### Magento ######
magento_sku_master:	
	cat queries/magento/magento_sku_master.sql | redshift

###### UNLOAD #######
unload_soh_report:
	cat queries/unload/unload_soh_report.sql | redshift
	$(S3CMD) cp s3://btq-etl/redshift/soh_report/soh_report_000 /tmp/soh_report.tsv
	zip -e -P BqBRn5Zy6z /tmp/soh_report.zip /tmp/soh_report.tsv
	$(S3CMD) cp /tmp/soh_report.zip s3://btqbi/redshift/soh_report/soh_report.zip --acl public-read

unload_product_catalog_report:
	cat queries/unload/unload_product_catalog_report.sql | redshift
	$(S3CMD) cp s3://btq-etl/redshift/product_catalog_report/product_catalog_report_000 /tmp/product_catalog_report.tsv
	zip -e -P BqBRn5Zy6z /tmp/product_catalog_report.zip /tmp/product_catalog_report.tsv
	$(S3CMD) cp /tmp/product_catalog_report.zip s3://btqbi/amit/product_catalog_report/product_catalog_report.zip --acl public-read

