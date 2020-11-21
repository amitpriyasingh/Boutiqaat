SHELL := /bin/bash
include /credentials/var.env
export
SQOOP = sqoop --options-file /credentials/importML.txt
S3BUCKET=s3://btq-etl/ML
S3CMD=s3cmd -c /credentials/s3-config-btqetl
current_month = $(shell date +"%m")
current_year = $(shell date +"%Y")

ifndef DATES
override DATES=$(shell date --date="1 day ago" +%Y%m%d)-$(shell date --date="1 day ago" +%Y%m%d)
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
		--query "$(shell cat queries/$(TABLE)/$(TABLE)_query.sql )"
	$(S3CMD) sync --exclude '*.crc' --exclude '_SUCCESS' --delete-removed /tmp/$(TABLE)/ ${S3BUCKET}/$(TABLE)/csv/

#dumping database full table with a query into S3 bucket
do_table_csv_partial:
	export TABLE=$(TABLE)
	export DATE=$(DATE)
	$(SQOOP) --target-dir /tmp/$(TABLE)/$(YEAR)/$(MONTH)/$(shell echo $(DATE) | cut -c7-)/ \
		--fields-terminated-by '\t' \
		--compress \
		--split-by $(SPLITBY) \
		--escaped-by \\ \
		--num-mappers 8 \
		--query "$(shell cat queries/$(TABLE)/$(TABLE)_part.sql| env DATE=$(DATE) python3 -m pybashutil.render)"
	$(S3CMD) sync --exclude '*.crc' --exclude '_SUCCESS' --delete-removed /tmp/$(TABLE)/$(YEAR)/$(MONTH)/$(shell echo $(DATE) | cut -c7-)/ ${S3BUCKET}/$(TABLE)/part/csv/$(YEAR)/$(MONTH)/$(shell echo $(DATE) | cut -c7-)/

#dumping database full table with a query into S3 bucket
do_table_csv_incremental:
	export TABLE=$(TABLE)
	export LAST_VALUE="$(LAST_VALUE)"
	mkdir -p /tmp/$(TABLE)/
	$(SQOOP) --target-dir /tmp/$(TABLE)/ \
		--fields-terminated-by '\t' \
		--compress \
		--split-by $(SPLITBY) \
		--escaped-by \\ \
		--check-column $(CHECK_COLUMN) \
		--incremental append \
		--last-value "$(LAST_VALUE)" \
		--direct \
		--query "$(shell cat queries/$(TABLE)/$(TABLE)_query_inc.sql)"
	$(S3CMD) sync --exclude '*.crc' --exclude '_SUCCESS' --delete-removed --force /tmp/$(TABLE)/ ${S3BUCKET}/$(TABLE)/incremental/csv/

do_table_csv_incremental_last_modified:
	export TABLE=$(TABLE)
	export LAST_VALUE="$(LAST_VALUE)"
	mkdir -p /tmp/$(TABLE)/
	$(SQOOP) --target-dir /tmp/$(TABLE)/ \
		--fields-terminated-by '\t' \
		--compress \
		--split-by $(SPLITBY) \
		--escaped-by \\ \
		--check-column $(CHECK_COLUMN) \
		--incremental lastmodified \
		--last-value "$(LAST_VALUE)" \
		--direct \
		--query "$(shell cat queries/$(TABLE)/$(TABLE)_query_inc.sql)"
	$(S3CMD) sync --exclude '*.crc' --exclude '_SUCCESS' --delete-removed --force /tmp/$(TABLE)/ ${S3BUCKET}/$(TABLE)/incremental/csv/

do_full: do_full_dump do_full_load

do_full_dump:
	$(MAKE) do_table_csv_full TABLE=$(TABLE) SPLITBY=$(SPLIT_COLUMN)

do_full_load:
	cat queries/$(TABLE)/$(TABLE)_load.sql | env S3PATH=${S3BUCKET}/$(TABLE)/csv/ python3 -m pybashutil.render | redshift

do_incremental: do_incremental_dump do_incremental_load

do_incremental_dump:
	export TABLE=$(TABLE)
	export MAX_COLUMN=$(MAX_COLUMN)
	$(MAKE) do_table_csv_incremental TABLE=$(TABLE) LAST_VALUE="$(shell redshift -t -c "select max(date_part(epoch, $(MAX_COLUMN))) from magento.$(TABLE)")" SPLITBY=$(SPLIT_COLUMN) CHECK_COLUMN=$(CHECK_COLUMN)

do_incremental_load:
	cat queries/$(TABLE)/$(TABLE)_incr.sql | env S3PATH=${S3BUCKET}/$(TABLE)/incremental/csv/ python3 -m pybashutil.render | redshift

do_incremental_lm: do_incremental_lm_dump do_incremental_lm_load

do_incremental_lm_dump:
	export TABLE=$(TABLE)
	export MAX_COLUMN=$(MAX_COLUMN)
	$(MAKE) do_table_csv_incremental_last_modified TABLE=$(TABLE) LAST_VALUE="$(shell redshift -t -c "select max($(MAX_COLUMN)) from magento.$(TABLE)")" SPLITBY=$(SPLIT_COLUMN) CHECK_COLUMN=$(CHECK_COLUMN)	

do_incremental_lm_load:
	cat queries/$(TABLE)/$(TABLE)_incr.sql | env S3PATH=${S3BUCKET}/$(TABLE)/incremental/csv/ python3 -m pybashutil.render | redshift

################################ Tables ############################

magento_daily: catalog_product_entity catalog_product_entity_int catalog_product_entity_varchar catalog_product_relation catalog_product_flat_1 disabled_skus eav_attribute eav_attribute_option_swatch eav_attribute_option_value sales_order sales_order_item magento_customerbalance magento_customerbalance_history celebrity_product celebrity_am_log magento_sku_master magento_product_catalog

# order_queue
order_queue:
	$(MAKE) do_full TABLE=order_queue TABLE_NAME='boutiqaat_middlelayer.order_queue WHERE created_time >= UNIX_TIMESTAMP(DATE_ADD(CURDATE(),INTERVAL -20 DAY))' SPLIT_COLUMN=id

# order_queue
order_queue_incremental:
	$(MAKE) do_incremental TABLE=order_queue TABLE_NAME='boutiqaat_middlelayer.order_queue' SPLIT_COLUMN=id CHECK_COLUMN=last_updated MAX_COLUMN=last_updated

# order_item_queue
order_item_queue:
	$(MAKE) do_full TABLE=order_item_queue TABLE_NAME='boutiqaat_middlelayer.order_item_queue WHERE created_time >= UNIX_TIMESTAMP(DATE_ADD(CURDATE(),INTERVAL -20 DAY))' SPLIT_COLUMN=id

# payment_gateway_report
payment_gateway_report:
	$(MAKE) do_full TABLE=payment_gateway_report TABLE_NAME='boutiqaat_middlelayer.order_queue' SPLIT_COLUMN=id	

payment_gateway_report_incremental:
	$(MAKE) do_incremental_lm TABLE=payment_gateway_report TABLE_NAME='boutiqaat_middlelayer.order_queue' SPLIT_COLUMN=id CHECK_COLUMN=last_updated MAX_COLUMN=last_updated
