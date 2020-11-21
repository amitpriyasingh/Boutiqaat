SHELL := /bin/bash
include /credentials/var.env
export
SQOOP = sqoop --options-file /credentials/importMagento.txt
S3BUCKET=s3://btq-etl/magento
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
	mkdir -p /tmp/$(TABLE)/
	$(SQOOP) --target-dir /tmp/$(TABLE)/ \
		--fields-terminated-by '\t' \
		--compress \
		--split-by $(SPLITBY) \
		--escaped-by \\ \
		--check-column $(CHECK_COLUMN) \
		--incremental append \
		--last-value "$(LAST_VALUE)" \
		--query "$(shell cat queries/$(TABLE)/$(TABLE)_query.sql )"
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
	$(MAKE) do_table_csv_incremental TABLE=$(TABLE) LAST_VALUE="$(shell redshift -t -c "select max($(MAX_COLUMN)) from magento.$(TABLE)")" SPLITBY=$(SPLIT_COLUMN) CHECK_COLUMN=$(CHECK_COLUMN)

do_incremental_load:
	cat queries/$(TABLE)/$(TABLE)_incr.sql | env S3PATH=${S3BUCKET}/$(TABLE)/incremental/csv/ python3 -m pybashutil.render | redshift



################################ Tables ############################

magento_daily: catalog_product_flat_1 disabled_skus eav_attribute eav_attribute_option_swatch sales_order sales_order_item magento_customerbalance magento_customerbalance_history celebrity_am_log magento_product_catalog sku_brands sku_categories customer_demographic_info notify_out_of_stock order_gen_mapped

# catalog_product_entity
catalog_product_entity:
	$(MAKE) do_full TABLE=catalog_product_entity TABLE_NAME=boutiqaat_v2.catalog_product_entity SPLIT_COLUMN=row_id

# catalog_product_entity_int
catalog_product_entity_int:
	$(MAKE) do_full TABLE=catalog_product_entity_int TABLE_NAME=boutiqaat_v2.catalog_product_entity_int SPLIT_COLUMN=value_id

# catalog_product_entity_varchar
catalog_product_entity_varchar:
	$(MAKE) do_full TABLE=catalog_product_entity_varchar TABLE_NAME=boutiqaat_v2.catalog_product_entity_varchar SPLIT_COLUMN=value_id

# catalog_product_entity_text
catalog_product_entity_text:
	$(MAKE) do_full TABLE=catalog_product_entity_text TABLE_NAME=boutiqaat_v2.catalog_product_entity_text SPLIT_COLUMN=value_id

# catalog_product_entity_decimal
catalog_product_entity_decimal:
	$(MAKE) do_full TABLE=catalog_product_entity_decimal TABLE_NAME=boutiqaat_v2.catalog_product_entity_decimal SPLIT_COLUMN=value_id

# catalog_product_relation
catalog_product_relation:
	$(MAKE) do_full TABLE=catalog_product_relation TABLE_NAME=boutiqaat_v2.catalog_product_relation SPLIT_COLUMN=parent_id

# catalog_product_flat_1
catalog_product_flat_1:
	$(MAKE) do_full TABLE=catalog_product_flat_1 TABLE_NAME=boutiqaat_v2.catalog_product_flat_1 SPLIT_COLUMN=entity_id

# celebrity_product
celebrity_product:
	$(MAKE) do_full TABLE=celebrity_product TABLE_NAME=boutiqaat_v2.celebrity_product SPLIT_COLUMN=product_id

# disabled_skus
disabled_skus:
	$(MAKE) do_full TABLE=disabled_skus TABLE_NAME=boutiqaat_v2.disabled_skus SPLIT_COLUMN=id

# sales_order
sales_order:
	$(MAKE) do_full TABLE=sales_order TABLE_NAME=boutiqaat_v2.sales_order SPLIT_COLUMN=entity_id

# sales_order_item
sales_order_item:
	$(MAKE) do_full TABLE=sales_order_item TABLE_NAME=boutiqaat_v2.sales_order_item SPLIT_COLUMN=item_id

# eav_attribute
eav_attribute:
	$(MAKE) do_full TABLE=eav_attribute TABLE_NAME=boutiqaat_v2.eav_attribute SPLIT_COLUMN=attribute_id

# eav_attribute_option_swatch
eav_attribute_option_swatch:
	$(MAKE) do_full TABLE=eav_attribute_option_swatch TABLE_NAME=boutiqaat_v2.eav_attribute_option_swatch SPLIT_COLUMN=option_id

# eav_attribute_option_value
eav_attribute_option_value:
	$(MAKE) do_full TABLE=eav_attribute_option_value TABLE_NAME=boutiqaat_v2.eav_attribute_option_value SPLIT_COLUMN=value_id

# magento_customerbalance
magento_customerbalance:
	$(MAKE) do_full TABLE=magento_customerbalance TABLE_NAME=boutiqaat_v2.magento_customerbalance SPLIT_COLUMN=balance_id

# magento_customerbalance_history
magento_customerbalance_history:
	$(MAKE) do_full TABLE=magento_customerbalance_history TABLE_NAME=boutiqaat_v2.magento_customerbalance_history SPLIT_COLUMN=history_id

# magento_sku_master
#magento_sku_master:
#	$(MAKE) do_full TABLE=magento_sku_master TABLE_NAME='(SELECT row_id FROM boutiqaat_v2.catalog_product_entity) as cp' SPLIT_COLUMN=cp.row_id

# sku_master
sku_master:
	$(MAKE) do_full TABLE=sku_master TABLE_NAME=dwh.sku_master SPLIT_COLUMN=created_date

# celebrity_am_log
celebrity_am_log:
	$(MAKE) do_full TABLE=celebrity_am_log TABLE_NAME=boutiqaat_v2.celebrity_am_log SPLIT_COLUMN=id

# magento_product_catalog
magento_product_catalog:
	$(MAKE) do_full TABLE=magento_product_catalog TABLE_NAME='boutiqaat_v2.catalog_product_entity as cp' SPLIT_COLUMN=cp.row_id

# magento_sku_brands
sku_brands:
	$(MAKE) do_full TABLE=sku_brands TABLE_NAME='(select cpei.value as brand_id from boutiqaat_v2.catalog_product_entity cpe  JOIN boutiqaat_v2.catalog_product_entity_int cpei ON cpe.row_id = cpei.row_id  JOIN  boutiqaat_v2.brand_management bm on cpei.value = bm.brand_option_id WHERE cpei.attribute_id = 81) as sub' SPLIT_COLUMN=brand_id

# magento_sku_categories
sku_categories:
	$(MAKE) do_full TABLE=sku_categories TABLE_NAME='(SELECT t1.category_id as category_id FROM boutiqaat_v2.catalog_category_product AS t1 JOIN boutiqaat_v2.catalog_category_entity AS t2 ON t1.category_id = t2.entity_id JOIN boutiqaat_v2.catalog_category_entity_varchar AS t3 ON t2.row_id = t3.row_id AND t3.attribute_id = 41 AND t3.store_id = 0 JOIN boutiqaat_v2.catalog_product_entity AS t4 ON t4.row_id=t1.product_id WHERE t1.category_id > 2) as sub' SPLIT_COLUMN=category_id

# sku_status_history
sku_status_history:
	$(MAKE) do_full TABLE=sku_status_history TABLE_NAME=boutiqaat_v2.sku_status_history SPLIT_COLUMN=id

# customer_demographic_info
customer_demographic_info:
	$(MAKE) do_full TABLE=customer_demographic_info TABLE_NAME='boutiqaat_v2.customer_grid_flat as cgf' SPLIT_COLUMN=cgf.entity_id

# notify_out_of_stock
notify_out_of_stock:
	$(MAKE) do_full TABLE=notify_out_of_stock TABLE_NAME=boutiqaat_v2.notify_out_of_stock SPLIT_COLUMN=id

#order_gen_mapped
order_gen_mapped:
	$(MAKE) do_full TABLE=order_gen_mapped TABLE_NAME='(SELECT cast(increment_id as UNSIGNED) as order_number FROM boutiqaat_v2.sales_order ) so' SPLIT_COLUMN=so.order_number

#order_gen_mapped_incremental
order_gen_mapped_incremental:
	$(MAKE) do_incremental TABLE=order_gen_mapped TABLE_NAME='(SELECT cast(increment_id as UNSIGNED) as order_number FROM boutiqaat_v2.sales_order ) so' SPLIT_COLUMN=so.order_number MAX_COLUMN=order_number CHECK_COLUMN=order_number

