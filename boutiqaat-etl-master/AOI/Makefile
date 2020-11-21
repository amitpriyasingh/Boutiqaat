SHELL := /bin/bash
include /credentials/var.env
export
SQOOP = sqoop --options-file /credentials/importOFS.txt
S3BUCKET=s3://btq-etl/AOI
S3CMD=s3cmd -c /credentials/s3-config-btqetl
current_month = $(shell date +"%m")
current_year = $(shell date +"%Y")

ifndef DATES
override DATES=$(shell date --date="0 day ago" +%Y%m%d)-$(shell date --date="0 day ago" +%Y%m%d)
endif

ifndef PDATE
override PDATE=$(shell date --date="5 day ago" +%Y-%m-%d)
endif

# running commands with a range of dates
daterun:
	python3 -m pybashutil.yearmonthdayrun $(DATES) make do_$(TABLE)_partial DATE='__DATE__' MONTH='__MONTH__' YEAR='__YEAR__'

#dumping database full table with a query into S3 bucket
do_table_csv_full:
	export TABLE=$(TABLE)
	$(SQOOP) --target-dir /tmp/$(TABLE)/ \
		--fields-terminated-by '\t' \
		--compress \
		--split-by $(SPLITBY) \
		--boundary-query 'SELECT min($(SPLITBY)), max($(SPLITBY)) FROM $(TABLE_NAME)' \
		--escaped-by \\ \
		--num-mappers 8 \
		--direct \
		--query "$(shell cat queries/$(TABLE)/$(TABLE)_query.sql )"
	$(S3CMD) sync --exclude '*.crc' --exclude '_SUCCESS' --delete-removed /tmp/$(TABLE)/ ${S3BUCKET}/$(TABLE)/csv/

#dumping database full table with a query into S3 bucket
do_table_csv_partial:
	export TABLE=$(TABLE)
	export PDATE=$(PDATE)
	$(SQOOP) --target-dir /tmp/$(TABLE)/ \
		--fields-terminated-by '\t' \
		--compress \
		--split-by $(SPLITBY) \
		--boundary-query 'SELECT min($(SPLITBY)), max($(SPLITBY)) FROM $(TABLE_NAME) WHERE $(CHECK_COLUMN) >= "$(PDATE)"' \
		--escaped-by \\ \
		--num-mappers 8 \
		--direct \
		--query "$(shell cat queries/$(TABLE)/$(TABLE)_query.sql) AND $(CHECK_COLUMN) >= '$(PDATE)'"
	$(S3CMD) sync --exclude '*.crc' --exclude '_SUCCESS' --delete-removed /tmp/$(TABLE)/ ${S3BUCKET}/$(TABLE)/partial/csv/

#dumping database full table with a query into S3 bucket
do_table_csv_incremental:
	export TABLE=$(TABLE)
	mkdir -p /tmp/$(TABLE)/
	$(SQOOP) --target-dir /tmp/$(TABLE)/ \
		--fields-terminated-by '\t' \
		--compress \
		--split-by $(SPLITBY) \
		--escaped-by \\ \
		--num-mappers 1 \
		--check-column $(CHECK_COLUMN) \
		--incremental append \
		--last-value "$(LAST_VALUE)" \
		--direct \
		--query "$(shell cat queries/$(TABLE)/$(TABLE)_query.sql ) AND $(CHECK_COLUMN) >= '$(LAST_VALUE)'"
	$(S3CMD) sync --exclude '*.crc' --exclude '_SUCCESS' --delete-removed --force /tmp/$(TABLE)/ ${S3BUCKET}/$(TABLE)/incremental/csv/

do_table_csv_lastmodified:
	export TABLE=$(TABLE)
	mkdir -p /tmp/$(TABLE)/
	$(SQOOP) --target-dir /tmp/$(TABLE)/ \
		--fields-terminated-by '\t' \
		--compress \
		--split-by $(SPLITBY) \
		--escaped-by \\ \
		--num-mappers 1 \
		--check-column $(CHECK_COLUMN) \
		--incremental lastmodified \
		--last-value "$(LAST_VALUE)" \
		--append \
		--direct \
		--query "$(shell cat queries/$(TABLE)/$(TABLE)_query.sql ) AND $(CHECK_COLUMN) >= '$(LAST_VALUE)'"
	$(S3CMD) sync --exclude '*.crc' --exclude '_SUCCESS' --delete-removed --force /tmp/$(TABLE)/ ${S3BUCKET}/$(TABLE)/lastmodified/csv/

do_full: do_full_dump do_full_load

do_full_dump:
	$(MAKE) do_table_csv_full TABLE=$(TABLE) SPLITBY=$(SPLIT_COLUMN)

do_full_load:
	export TABLE=$(TABLE)
	cat queries/$(TABLE)/$(TABLE)_load.sql | env S3PATH=${S3BUCKET}/$(TABLE)/csv/ SYNC="$(shell cat ../tools/sync/query.sql | env SCHEMA=AOI TABLE=$(TABLE)  python3 -m pybashutil.render)" python3 -m pybashutil.render | redshift

do_incremental: do_incremental_dump do_incremental_load

do_incremental_dump:
	export TABLE=$(TABLE)
	export MAX_COLUMN=$(MAX_COLUMN)
	$(MAKE) do_table_csv_incremental TABLE=$(TABLE) LAST_VALUE="$(shell redshift -t -c "select max($(MAX_COLUMN)) from aoi.$(TABLE)")" SPLITBY=$(SPLIT_COLUMN) CHECK_COLUMN=$(CHECK_COLUMN)

do_incremental_load:
	export TABLE=$(TABLE)
	cat queries/$(TABLE)/$(TABLE)_incr.sql | env S3PATH=${S3BUCKET}/$(TABLE)/incremental/csv/ SYNC="$(shell cat ../tools/sync/query.sql | env SCHEMA=AOI TABLE=$(TABLE)  python3 -m pybashutil.render)" python3 -m pybashutil.render | redshift

do_lastmodified: do_lastmodified_dump do_lastmodified_load

do_lastmodified_dump:
	export TABLE=$(TABLE)
	export MAX_COLUMN=$(MAX_COLUMN)
	$(MAKE) do_table_csv_lastmodified TABLE=$(TABLE) LAST_VALUE="$(shell redshift -t -c "select max($(MAX_COLUMN)) from aoi.$(TABLE)")" SPLITBY=$(SPLIT_COLUMN) CHECK_COLUMN=$(CHECK_COLUMN)

do_lastmodified_load:
	export TABLE=$(TABLE)
	cat queries/$(TABLE)/$(TABLE)_lm.sql | env S3PATH=${S3BUCKET}/$(TABLE)/lastmodified/csv/ python3 -m pybashutil.render | redshift

do_partial: do_partial_dump do_partial_load

do_partial_dump:
	export TABLE=$(TABLE)
	$(MAKE) do_table_csv_partial TABLE=$(TABLE) SPLITBY=$(SPLIT_COLUMN) CHECK_COLUMN=$(CHECK_COLUMN)

do_partial_load:
	export TABLE=$(TABLE)
	cat queries/$(TABLE)/$(TABLE)_part.sql | env S3PATH=${S3BUCKET}/$(TABLE)/partial/csv/ PDATE=$(PDATE) SYNC="$(shell cat ../tools/sync/query.sql | env SCHEMA=AOI TABLE=$(TABLE)  python3 -m pybashutil.render)" python3 -m pybashutil.render | redshift

aoi_daily: order_items order_details order_items brand_sales bi_celebrity_master brand_target_sales_master_details celebrity_master celebrity_mtd_sale_tab celebrity_daily_target soh_report

aoi_hourly: consolidated_sku_stock inventory_health aoi_orders_partial sku_stock

aoi_orders_partial: order_items_partial order_details_partial

aoi_orders_incremental: order_items_incremental order_details_incremental


# email_uuid_r1
# email_uuid_r1:
# 	$(MAKE) do_full TABLE=email_uuid_r1 TABLE_NAME=aoi.email_uuid_r1 SPLIT_COLUMN=uuid

# phone_uuid_r1
# phone_uuid_r1:
# 	$(MAKE) do_full TABLE=phone_uuid_r1 TABLE_NAME=aoi.phone_uuid_r1 SPLIT_COLUMN=uuid

# order_items
order_items:
	$(MAKE) do_full TABLE=order_items TABLE_NAME=aoi.order_items SPLIT_COLUMN=item_id

# order_items
order_items_partial:
	$(MAKE) do_partial TABLE=order_items TABLE_NAME=aoi.order_items SPLIT_COLUMN=item_id CHECK_COLUMN=order_at

# order_items
order_items_incremental:
	$(MAKE) do_incremental TABLE=order_items TABLE_NAME=aoi.order_items SPLIT_COLUMN=item_id CHECK_COLUMN=order_at MAX_COLUMN=order_at

# order_items
order_items_lastmodified:
	$(MAKE) do_lastmodified TABLE=order_items TABLE_NAME=aoi.order_items SPLIT_COLUMN=item_id CHECK_COLUMN=updated_at_utc MAX_COLUMN=updated_at_utc

# order_details
order_details:
	$(MAKE) do_full TABLE=order_details TABLE_NAME=aoi.order_details SPLIT_COLUMN=item_id

# order_details partial
order_details_partial:
	$(MAKE) do_partial TABLE=order_details TABLE_NAME=aoi.order_details SPLIT_COLUMN=item_id CHECK_COLUMN=order_at

# order_details partial
order_details_incremental:
	$(MAKE) do_incremental TABLE=order_details TABLE_NAME=aoi.order_details SPLIT_COLUMN=item_id CHECK_COLUMN=order_at MAX_COLUMN=order_at

order_details_lastmodified:
	$(MAKE) do_lastmodified TABLE=order_details TABLE_NAME=aoi.order_details SPLIT_COLUMN=item_id CHECK_COLUMN=updated_at_utc MAX_COLUMN=updated_at_utc

# events_report
# events_report:
# 	$(MAKE) do_full TABLE=events_report TABLE_NAME=aoi.events_report SPLIT_COLUMN=id

# consolidated_sku_stock
consolidated_sku_stock:
	$(MAKE) do_full TABLE=consolidated_sku_stock TABLE_NAME=aoi.consolidated_sku_stock SPLIT_COLUMN=report_at_AST
	
#inventory_health	
inventory_health:
	$(MAKE) do_full TABLE=inventory_health TABLE_NAME=aoi.inventory_health SPLIT_COLUMN=soh	

#sku_stock	
sku_stock:
	$(MAKE) do_full TABLE=sku_stock TABLE_NAME=aoi.sku_stock SPLIT_COLUMN=updated_at

#soh_report	
soh_report:
	$(MAKE) do_full TABLE=soh_report TABLE_NAME="(SELECT CAST((@row_number:=@row_number + 1) as UNSIGNED) AS num FROM aoi.soh_report, (SELECT @row_number:=0) AS t) as sub" SPLIT_COLUMN=num	
	
# brand_sales
brand_sales:
	$(MAKE) do_full TABLE=brand_sales TABLE_NAME=aoi.brand_sales SPLIT_COLUMN=order_date

# bi_celebrity_master
bi_celebrity_master:
	$(MAKE) do_full TABLE=bi_celebrity_master TABLE_NAME=aoi.bi_celebrity_master SPLIT_COLUMN=entry_id


# brand_target_sales_master_details
brand_target_sales_master_details:
	$(MAKE) do_full TABLE=brand_target_sales_master_details TABLE_NAME=aoi.brand_target_sales_master_details SPLIT_COLUMN=traget_month

# celebrity_master
celebrity_master:
	$(MAKE) do_full_dump TABLE=celebrity_master TABLE_NAME=aoi.celebrity_master SPLIT_COLUMN=celebrity_id
	
# celebrity_mtd_sale_tab
celebrity_mtd_sale_tab:
	$(MAKE) do_full TABLE=celebrity_mtd_sale_tab TABLE_NAME=aoi.celebrity_mtd_sale_tab SPLIT_COLUMN=celebrity_id	

# celebrity_daily_target
celebrity_daily_target:
	$(MAKE) do_full TABLE=celebrity_daily_target TABLE_NAME=aoi.celebrity_daily_target SPLIT_COLUMN=target_date	

# dsp_shipment_status
dsp_shipment_status:
	$(MAKE) do_full TABLE=dsp_shipment_status TABLE_NAME=aoi.dsp_shipment_status SPLIT_COLUMN=order_date
	
# celebrity_emt_report
celebrity_emt_report:
	$(MAKE) do_full_dump TABLE=celebrity_emt_report TABLE_NAME=aoi.celebrity_emt_report SPLIT_COLUMN=events_header_id
