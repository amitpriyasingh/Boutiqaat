SHELL := /bin/bash
include /credentials/var.env
export
SQOOP = sqoop --options-file /credentials/importBIDB.txt
S3BUCKET=s3://btq-etl/BIDB
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
	cat queries/$(TABLE)/$(TABLE)_load.sql | env S3PATH=${S3BUCKET}/$(TABLE)/csv/ python3 -m pybashutil.render | redshift

do_incremental: do_incremental_dump do_incremental_load

do_incremental_dump:
	export TABLE=$(TABLE)
	export MAX_COLUMN=$(MAX_COLUMN)
	$(MAKE) do_table_csv_incremental TABLE=$(TABLE) LAST_VALUE="$(shell redshift -t -c "select max($(MAX_COLUMN)) from ofs.$(TABLE)")" SPLITBY=$(SPLIT_COLUMN) CHECK_COLUMN=$(CHECK_COLUMN)

do_incremental_load:
	cat queries/$(TABLE)/$(TABLE)_incr.sql | env S3PATH=${S3BUCKET}/$(TABLE)/incremental/csv/ python3 -m pybashutil.render | redshift

####################################################################
####################################################################
####################################################################


# emt_report
emt_report:
	$(MAKE) do_full TABLE=emt_report TABLE_NAME=events.emt_report SPLIT_COLUMN=event_date

test_rule:
	cat queries/emt_report/emt_report_query.sql