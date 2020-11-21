SHELL := /bin/bash
include /credentials/var.env
export
SQOOP = sqoop --options-file /credentials/importNAV.txt
S3BUCKET=s3://btq-etl/NAV
S3CMD=s3cmd -c /credentials/s3-config-btqetl
current_month = $(shell date +"%m")
current_year = $(shell date +"%Y")

# running commands with a range of dates
daterun:
	python3 -m pybashutil.yearmonthdayrun $(DATES) make do_table_avro_part DATE='__DATE__' MONTH='__MONTH__' YEAR='__YEAR__'

#dumping database full table with a query into S3 bucket
do_table_csv_full_old:
	export TABLE=$(TABLE)
	$(SQOOP) --target-dir /tmp/$(TABLE)/ \
		--fields-terminated-by '\t' \
		--compress \
		--escaped-by \\ \
		--split-by $(SPLITBY) \
		--num-mappers 8 \
		--query "$(shell cat queries/$(TABLE)/$(TABLE)_query.sql )"
	$(S3CMD) sync --exclude '*.crc' --exclude '_SUCCESS' /tmp/$(TABLE)/ ${S3BUCKET}/$(TABLE)/csv/


#dumping database table with a query and date boundries into S3 bucket following partition format
do_table_avro_part:
	export DATE=$(DATE)
	export TABLE=$(TABLE)
	$(SQOOP) --target-dir /tmp/$(TABLE)/$(YEAR)/$(MONTH)/$(shell echo $(DATE) | cut -c7-)/ --as-avrodatafile --split-by rownum --query "$(shell cat queries/$(TABLE)/$(TABLE)_part.sql| env DATE=$(DATE) python3 -m pybashutil.render)"
	$(S3CMD) sync --exclude '*.crc' --exclude '_SUCCESS' --delete-removed /tmp/$(TABLE)/$(YEAR)/$(MONTH)/$(shell echo $(DATE) | cut -c7-)/ ${S3BUCKET}/$(TABLE)/avro/$(YEAR)/$(MONTH)/$(shell echo $(DATE) | cut -c7-)/

#dumping database full table with a query into S3 bucket
do_table_csv_full:
	export TABLE=$(TABLE)
	$(SQOOP) --target-dir /tmp/$(TABLE)/ \
		--fields-terminated-by '\t' \
		--compress \
		--split-by '$(SPLITBY)' \
		--boundary-query 'SELECT min($(SPLITBY)), max($(SPLITBY)) FROM Boutiqaat_Live.dbo.[Boutiqaat Kuwait$$$(TABLE_NAME)] $(ALIAS)' \
		--escaped-by \\ \
		--num-mappers 8 \
		--query "$(shell cat queries/$(TABLE)/$(TABLE)_query.sql )"
	$(S3CMD) sync --exclude '*.crc' --exclude '_SUCCESS' --delete-removed /tmp/$(TABLE)/ ${S3BUCKET}/$(TABLE)/csv/

do_table_csv_incremental:
	export TABLE=$(TABLE)
	mkdir -p /tmp/$(TABLE)/
	$(SQOOP) --target-dir /tmp/$(TABLE)/ \
		--fields-terminated-by '\t' \
		--compress \
		--split-by '$(SPLITBY)' \
		--escaped-by \\ \
		--enclosed-by '\"' \
		--check-column '$(CHECK_COLUMN)' \
		--incremental append \
		--last-value $(LAST_VALUE) \
		--query "$(shell cat queries/$(TABLE)/$(TABLE)_query.sql ) AND $(CHECK_COLUMN) >= '$(LAST_VALUE)'"
	$(S3CMD) sync --exclude '*.crc' --exclude '_SUCCESS' --delete-removed  --force /tmp/$(TABLE)/ ${S3BUCKET}/$(TABLE)/incremental/csv/

do_full: do_full_dump do_full_load


do_full_dump:
	$(MAKE) do_table_csv_full TABLE=$(TABLE) SPLITBY='$(SPLIT_COLUMN)'

do_full_load:
	cat queries/$(TABLE)/$(TABLE)_load.sql | env S3PATH=${S3BUCKET}/$(TABLE)/csv/ python3 -m pybashutil.render | redshift

do_incremental: do_incremental_dump do_incremental_load

do_incremental_dump:
	export TABLE=$(TABLE)
	export MAX_COLUMN=$(MAX_COLUMN)
	$(MAKE) do_table_csv_incremental TABLE=$(TABLE) LAST_VALUE="$(shell redshift -t -c "select max($(MAX_COLUMN)) from nav.$(TABLE)")" SPLITBY='$(SPLIT_COLUMN)' CHECK_COLUMN='$(CHECK_COLUMN)'

do_incremental_load:
	cat queries/$(TABLE)/$(TABLE)_incr.sql | env S3PATH=${S3BUCKET}/$(TABLE)/incremental/csv/ python3 -m pybashutil.render | redshift


####################################################
################### item tables ####################
####################################################

#Boutiqaat Kuwait$Item
item:
	$(MAKE) do_full TABLE=item TABLE_NAME='Item' SPLIT_COLUMN='[Synced DateTime]'

#Boutiqaat Kuwait$Item Vendor
item_vendor:
	$(MAKE) do_full TABLE=item_vendor TABLE_NAME='Item Vendor' SPLIT_COLUMN='[Start Date]'

#Boutiqaat Kuwait$Item Ledger Entry
item_ledger_entry:
	$(MAKE) do_full TABLE=item_ledger_entry TABLE_NAME='Item Ledger Entry' SPLIT_COLUMN='[Posting Date]'

#Boutiqaat Kuwait$Item Vendor Discount
item_vendor_discount:
	$(MAKE) do_full TABLE=item_vendor_discount TABLE_NAME='Item Vendor Discount' SPLIT_COLUMN='[Start Date]'

####################################################
################# purchase tables ##################
####################################################

#Boutiqaat Kuwait$Purch_ Rcpt_ Header
purch_rcpt_header:
	$(MAKE) do_full TABLE=purch_rcpt_header TABLE_NAME='Purch_ Rcpt_ Header' SPLIT_COLUMN='[Posting Date]'

#Boutiqaat Kuwait$Purch_ Rcpt_ Line
purch_rcpt_line:
	$(MAKE) do_full TABLE=purch_rcpt_line TABLE_NAME='Purch_ Rcpt_ Line' SPLIT_COLUMN='[Posting Date]'

#Boutiqaat Kuwait$Purchase Header
purchase_header:
	$(MAKE) do_full TABLE=purchase_header TABLE_NAME='Purchase Header' SPLIT_COLUMN='[Posting Date]'

#Boutiqaat Kuwait$Purchase Line
purchase_line:
	$(MAKE) do_full TABLE=purchase_line TABLE_NAME='Purchase Line' SPLIT_COLUMN='[FA Posting Date]'

####################################################
################# sales tables #####################
####################################################

#Boutiqaat Kuwait$Sales Invoice Line
sales_invoice_line:
	$(MAKE) do_full TABLE=sales_invoice_line TABLE_NAME='Sales Invoice Line' SPLIT_COLUMN='[Shipment Date]'

####################################################
############### warehouse tables ###################
####################################################

#Boutiqaat Kuwait$Warehouse Entry
warehouse_entry:
	$(MAKE) do_full TABLE=warehouse_entry TABLE_NAME='Warehouse Entry' SPLIT_COLUMN='[Entry No_]'


warehouse_entry_incremental:
	$(MAKE) do_incremental TABLE=warehouse_entry SPLIT_COLUMN='[Entry No_]' MAX_COLUMN=entry_no CHECK_COLUMN='[Entry No_]'

#Boutiqaat Kuwait$Location
location:
	$(MAKE) do_full TABLE=location TABLE_NAME='Location' SPLIT_COLUMN='[Special Equipment]'

#Boutiqaat Kuwait$Stock Details
stock_details:
	$(MAKE) do_full TABLE=stock_details TABLE_NAME='Stock Details' SPLIT_COLUMN='[Entry No]'


## the following rules are for aggregated queries on NAV
## please ignore this as it will be removed in the future
#Aggregated


####################################
##### NAV SOH AGGREGATION ##########
####################################

# nav_sku_master
nav_sku_master_full:
	$(MAKE) do_full TABLE=nav_sku_master TABLE_NAME='Item' SPLIT_COLUMN='i.[No_]' ALIAS='i'


# nav_sku_master_full: nav_sku_master_full_dump nav_sku_master_full_load

# nav_sku_master_full_dump:
# 	$(MAKE) do_table_csv_full_old TABLE=nav_sku_master SPLITBY='row_num'

# nav_sku_master_full_load:
# 	cat queries/nav_sku_master/nav_sku_master_load.sql | env S3PATH=${S3BUCKET}/nav_sku_master/csv/ python3 -m pybashutil.render | redshift

# stock_agg_full:
# 	$(MAKE) do_full TABLE=stock_agg TABLE_NAME='Item' SPLIT_COLUMN='i.[No_]' ALIAS='i'


# stock_agg
stock_agg_full: stock_agg_full_dump stock_agg_full_load

stock_agg_full_dump:
	$(MAKE) do_table_csv_full_old TABLE=stock_agg SPLITBY=SOH

stock_agg_full_load:
	cat queries/stock_agg/stock_agg_load.sql | env S3PATH=${S3BUCKET}/stock_agg/csv/ python3 -m pybashutil.render | redshift

#nav_product_category
nav_product_category_full:
	$(MAKE) do_full TABLE=nav_product_category TABLE_NAME='Item' SPLIT_COLUMN='[No_]'


# nav_product_category_full: nav_product_category_full_dump nav_product_category_full_load

# nav_product_category_full_dump:
# 	$(MAKE) do_table_csv_full_old TABLE=nav_product_category SPLITBY=SOH

# nav_product_category_full_load:
# 	cat queries/nav_product_category/nav_product_category_load.sql | env S3PATH=${S3BUCKET}/nav_product_category/csv/ python3 -m pybashutil.render | redshift

#nav_sku_cost
nav_sku_cost: nav_sku_cost_full_dump nav_sku_cost_full_load

nav_sku_cost_full_dump:
	$(MAKE) do_table_csv_full_old TABLE=nav_sku_cost SPLITBY='max_posting_date'

nav_sku_cost_full_load:
	cat queries/nav_sku_cost/nav_sku_cost_load.sql | env S3PATH=${S3BUCKET}/nav_sku_cost/csv/ python3 -m pybashutil.render | redshift