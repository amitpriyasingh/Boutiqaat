SHELL := /bin/bash
include /credentials/var.env
export
SQOOP = sqoop --options-file /credentials/importOFS.txt
S3BUCKET=s3://btq-etl/OFS
S3CMD=s3cmd -c /credentials/s3-config-btqetl
current_month = $(shell date +"%m")
current_year = $(shell date +"%Y")

ifndef DATES
override DATES=$(shell date --date="0 day ago" +%Y%m%d)-$(shell date --date="0 day ago" +%Y%m%d)
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

do_table_agg_csv_full:
	export TABLE=$(TABLE)
	$(SQOOP) --target-dir /tmp/$(TABLE)/ \
		--fields-terminated-by '\t' \
		--compress \
		--split-by $(SPLITBY) \
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
		--boundary-query 'SELECT min($(SPLITBY)), max($(SPLITBY)) FROM $(TABLE_NAME)' \
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
		--num-mappers 1 \
		--check-column $(CHECK_COLUMN) \
		--incremental append \
		--last-value "$(LAST_VALUE)" \
		--query "$(shell cat queries/$(TABLE)/$(TABLE)_query.sql ) AND $(CHECK_COLUMN) >= '$(LAST_VALUE)'"
	$(S3CMD) sync --exclude '*.crc' --exclude '_SUCCESS' --delete-removed --force /tmp/$(TABLE)/ ${S3BUCKET}/$(TABLE)/incremental/csv/

do_full: do_full_dump do_full_load

do_full_dump:
	$(MAKE) do_table_csv_full TABLE=$(TABLE) SPLITBY=$(SPLIT_COLUMN)

do_full_load:
	export TABLE=$(TABLE)
	cat queries/$(TABLE)/$(TABLE)_load.sql | env S3PATH=${S3BUCKET}/$(TABLE)/csv/ SYNC="$(shell cat ../tools/sync/query.sql | env SCHEMA=OFS TABLE=$(TABLE)  python3 -m pybashutil.render)" python3 -m pybashutil.render | redshift

do_full_agg: do_full_agg_dump do_full_agg_load

do_full_agg_dump:
	$(MAKE) do_table_agg_csv_full TABLE=$(TABLE) SPLITBY=$(SPLIT_COLUMN)

do_full_agg_load:
	export TABLE=$(TABLE)
	cat queries/$(TABLE)/$(TABLE)_load.sql | env S3PATH=${S3BUCKET}/$(TABLE)/csv/ SYNC="$(shell cat ../tools/sync/query.sql | env SCHEMA=OFS TABLE=$(TABLE)  python3 -m pybashutil.render)" python3 -m pybashutil.render | redshift

do_incremental: do_incremental_dump do_incremental_load

do_incremental_dump:
	export TABLE=$(TABLE)
	export MAX_COLUMN=$(MAX_COLUMN)
	$(MAKE) do_table_csv_incremental TABLE=$(TABLE) LAST_VALUE="$(shell redshift -t -c "select max($(MAX_COLUMN)) from ofs.$(TABLE)")" SPLITBY=$(SPLIT_COLUMN) CHECK_COLUMN=$(CHECK_COLUMN)

do_incremental_load:
	export TABLE=$(TABLE)
	cat queries/$(TABLE)/$(TABLE)_incr.sql | env S3PATH=${S3BUCKET}/$(TABLE)/incremental/csv/ SYNC="$(shell cat ../tools/sync/query.sql | env SCHEMA=OFS TABLE=$(TABLE)  python3 -m pybashutil.render)" python3 -m pybashutil.render | redshift


##### sales rules

# InboundSalesHeader
inbound_sales_header:
	$(MAKE) do_full TABLE=inbound_sales_header TABLE_NAME=OFS.InboundSalesHeader SPLIT_COLUMN=Id

inbound_sales_header_incremental:
	$(MAKE) do_incremental TABLE=inbound_sales_header SPLIT_COLUMN=Id MAX_COLUMN=id CHECK_COLUMN=Id


# InboundSalesLine
inbound_sales_line:
	$(MAKE) do_full TABLE=inbound_sales_line TABLE_NAME=InboundSalesLine SPLIT_COLUMN=ItemId

inbound_sales_line_incremental:
	$(MAKE) do_incremental TABLE=inbound_sales_line TABLE_NAME=InboundSalesLine SPLIT_COLUMN=ItemId MAX_COLUMN=item_id CHECK_COLUMN=ItemId

# InboundOrderAddress
inbound_order_address:
	$(MAKE) do_full TABLE=inbound_order_address TABLE_NAME=OFS.InboundOrderAddress SPLIT_COLUMN=Id

inbound_order_address_incremental:
	$(MAKE) do_incremental TABLE=inbound_order_address TABLE_NAME=InboundOrderAddress SPLIT_COLUMN=Id MAX_COLUMN=id CHECK_COLUMN=Id

# InboundPaymentLine
inbound_payment_line:
	$(MAKE) do_full TABLE=inbound_payment_line TABLE_NAME=OFS.InboundPaymentLine SPLIT_COLUMN=Id

inbound_payment_line_incremental:
	$(MAKE) do_incremental TABLE=inbound_payment_line SPLIT_COLUMN=Id MAX_COLUMN=id CHECK_COLUMN=Id

# OrderBatchDetails
order_batch_details:
	$(MAKE) do_full TABLE=order_batch_details TABLE_NAME=OFS.OrderBatchDetails SPLIT_COLUMN=ItemId

order_batch_details_incremental:
	$(MAKE) do_incremental TABLE=order_batch_details SPLIT_COLUMN=InsertedOn MAX_COLUMN=inserted_on CHECK_COLUMN=InsertedOn

# CRMOrders
crm_orders:
	$(MAKE) do_full TABLE=crm_orders TABLE_NAME=OFS.CRMOrders SPLIT_COLUMN=Id

crm_orders_incremental:
	$(MAKE) do_incremental TABLE=crm_orders SPLIT_COLUMN=Id MAX_COLUMN=id CHECK_COLUMN=Id

# HoldOrders
hold_orders:
	$(MAKE) do_full TABLE=hold_orders TABLE_NAME=OFS.HoldOrders SPLIT_COLUMN=Id

hold_orders_incremental:
	$(MAKE) do_incremental TABLE=hold_orders SPLIT_COLUMN=Id MAX_COLUMN=Id CHECK_COLUMN=Id

# OrderStatus
order_status:
	$(MAKE) do_full TABLE=order_status TABLE_NAME=OFS.OrderStatus SPLIT_COLUMN=Id

order_status_incremental:
	$(MAKE) do_incremental TABLE=order_status SPLIT_COLUMN=Id MAX_COLUMN=id CHECK_COLUMN=Id


# CancelledOrders
cancelled_orders:
	$(MAKE) do_full TABLE=cancelled_orders TABLE_NAME=OFS.CancelledOrders SPLIT_COLUMN=Id

cancelled_orders_incremental:
	$(MAKE) do_incremental TABLE=cancelled_orders SPLIT_COLUMN=Id MAX_COLUMN=id  CHECK_COLUMN=Id

# StatusMaster
status_master:
	$(MAKE) do_full TABLE=status_master TABLE_NAME=OFS.StatusMaster SPLIT_COLUMN=id
