SHELL := /bin/bash
# include /credentials/var.env
# export
SQOOP = sqoop --options-file /credentials/importRedshift.txt
S3BUCKET=s3://btq-etl/adjust
S3ADJUST=s3://btq-adjust
S3CMD=aws s3
CURRENT_MONTH = $(shell date +"%m")
CURRENT_YEAR = $(shell date +"%Y")
ADJUST_DAY=$(shell date +%Y-%m-%d)

ifndef DATES
override DATES=$(shell date --date="1 day ago" +%Y%m%d)-$(shell date --date="1 day ago" +%Y%m%d)
endif

# running commands with a range of dates
daterun:
	python3 -m pybashutil.yearmonthdayrun $(DATES) make do_$(TABLE) DATE='__DATE__' MONTH='__MONTH__' YEAR='__YEAR__'

adjust_daily: redshift_load_external_adjust network_partners transactions attributes_actions sessions

adjust: s3_sync_adjust redshift_sync_adjust

# sync data from adjust s3 bucket to etl bucket in partitioned style
s3_sync_adjust:
	$(MAKE) daterun TABLE=s3_sync_adjust

do_s3_sync_adjust:
	export DATE=$(DATE)
	$(S3CMD) sync --exclude '*' --include '*$(YEAR)-$(MONTH)-$(shell echo $(DATE) | cut -c7-)*' --exclude '$(YEAR)$(MONTH)/*' $(S3ADJUST)/ $(S3BUCKET)/csv/$(YEAR)/$(MONTH)/$(shell echo $(DATE) | cut -c7-)/

# sync data from etl bucket to redshift
redshift_sync_adjust:
	$(MAKE) daterun TABLE=redshift_sync_adjust

do_redshift_sync_adjust:
	export DATE=$(DATE)
	cat queries/load_adjust.sql | env DATE=$(DATE) S3PATH=$(S3BUCKET)/csv/$(YEAR)/$(MONTH)/$(shell echo $(DATE) | cut -c7-)/ python3 -m pybashutil.render | redshift

# sync data from etl bucket to redshift
redshift_load_external_adjust:
	$(MAKE) daterun TABLE=redshift_load_external_adjust

do_redshift_load_external_adjust:
	export DATE=$(DATE)
	cat queries/load_external_adjust.sql | env DATE=$(DATE) S3PATH=$(S3BUCKET)/csv/$(YEAR)/$(MONTH)/$(shell echo $(DATE) | cut -c7-)/ python3 -m pybashutil.render | redshift
	aws s3 ls s3://btq-etl/adjust/parquet/ --recursive | grep ingestion_date=$(YEAR)-$(MONTH)-$(shell echo $(DATE) | cut -c7-) | awk '{print $$4}' | sed 's#/[^/]*$$##' > partitioned.txt
	sort partitioned.txt | uniq -d > sorted.txt
	while read p; do \
	$(MAKE) load_partition PARTPATH="s3://btq-etl/$$p"; \
	done < sorted.txt

load_partition:
	cat queries/load_external_partitions.sql | env PARTPATH="$(PARTPATH)" python3 -m pybashutil.render | redshift

export_redshift_adjust:
	$(MAKE) daterun TABLE=export_redshift_adjust

do_export_redshift_adjust:
	export DATE=$(DATE)
	$(SQOOP) --target-dir /tmp/adjust/$(YEAR)/$(MONTH)/$(shell echo $(DATE) | cut -c7-)/ \
		--as-parquetfile \
		--compression-codec org.apache.hadoop.io.compress.SnappyCodec \
		--driver com.amazon.redshift.jdbc41.Driver \
		--connection-manager org.apache.sqoop.manager.GenericJdbcManager \
		--split-by rownum \
		--num-mappers 16 \
		--query "$(shell cat queries/export_adjust.sql| env DATE=$(DATE) python3 -m pybashutil.render)"
	$(S3CMD) sync /tmp/adjust/$(YEAR)/$(MONTH)/$(shell echo $(DATE) | cut -c7-)/ $(S3BUCKET)/parquet/$(YEAR)/$(MONTH)/$(shell echo $(DATE) | cut -c7-)/

# sync data from etl bucket to redshift
network_partners:
	$(MAKE) daterun TABLE=network_partners

do_network_partners:
	export DATE=$(DATE)
	cat queries/network_partners.sql | env DATE=$(DATE) python3 -m pybashutil.render | redshift

transactions:
	$(MAKE) daterun TABLE=transactions

do_transactions:
	export DATE=$(DATE)
	cat queries/transactions.sql | env DATE=$(DATE) python3 -m pybashutil.render | redshift

attributes_actions:
	$(MAKE) daterun TABLE=attributes_actions

do_attributes_actions:
	export DATE=$(DATE)
	cat queries/attributes_actions.sql | env DATE=$(DATE) python3 -m pybashutil.render | redshift

sessions:
	$(MAKE) daterun TABLE=daily_sessions
	#$(MAKE) daterun TABLE=sessions_activity
	$(MAKE) daterun TABLE=channel_level_session_summary

do_daily_sessions:
	export DATE=$(DATE)
	cat queries/daily_sessions.sql | env DATE=$(DATE) python3 -m pybashutil.render | redshift

do_sessions_activity:
	export DATE=$(DATE)
	cat queries/sessions_activity.sql | env DATE=$(DATE) python3 -m pybashutil.render | redshift

do_channel_level_session_summary:
	export DATE=$(DATE)
	cat queries/channel_level_session_summary.sql | env DATE=$(DATE) python3 -m pybashutil.render | redshift

# sync data from etl bucket to redshift
load_external_network_partners:
	$(MAKE) daterun TABLE=load_external_network_partners

do_load_external_network_partners:
	export DATE=$(DATE)
	aws s3 ls s3://btq-etl/adjust/network_partners/ --recursive | grep ingestion_date=$(YEAR)-$(MONTH)-$(shell echo $(DATE) | cut -c7-) | awk '{print $$4}' | sed 's#/[^/]*$$##' > partitioned.txt
	sort partitioned.txt | uniq -d > sorted.txt
	while read p; do \
	$(MAKE) load_network_partners_partition PARTPATH="s3://btq-etl/$$p"; \
	done < sorted.txt

load_network_partners_partition:
	cat queries/load_external_network_partners.sql | env PARTPATH="$(PARTPATH)" python3 -m pybashutil.render | redshift

#### mixpanel data export job ######

# fetch mixpanel events data from API to etl S3 bucket
mixpanel_export_data:
	$(MAKE) daterun TABLE=mixpanel_export_data

do_mixpanel_export_data:
	curl https://data.mixpanel.com/api/2.0/export/ \
    	-u 19938a7d3082e104f13efea877f63105: \
    	-d from_date="$(YEAR)-$(MONTH)-$(shell echo $(DATE) | cut -c7-)" \
    	-d to_date="$(YEAR)-$(MONTH)-$(shell echo $(DATE) | cut -c7-)" \
    	-d event='["$$ae_session","$$al_nav_in","$$app_open","$$campaign_delivery","$$campaign_received","$$journey_entered","$$message_suppressed","Abandon Cart","Account Created","Add to Cart","Added to Cart","App Download","Browse","Complete Purchase","Delivery Method Added","Item Detail Page","Landing Page Loaded","Page Loaded","Payment Info Added","Promotion Clicked","Review Payment","Search","Share Item","Store Selected","abandon_cart","add_payment_info","add_to_cart","add_to_wishlist","apierrorlog","begin_checkout","bottom_navigation","checkout_progress","deeplink","delivery_address_added","delivery_method_added","ecommerce_purchase","filter","forgot_password","item Detail Page","login","my_account","oops","page_loaded","promotion_click","remove_from_cart","search","select_content","share","share_item","side_navigation","sign_up","store_selected","view_search_results"]' > /tmp/mixpanel_$(DATE).json
	$(S3CMD) cp /tmp/mixpanel_$(DATE).json s3://btq-etl/mixpanel/json/$(YEAR)/$(MONTH)/$(shell echo $(DATE) | cut -c7-)/mixpanel_$(DATE).json
	rm /tmp/mixpanel_$(DATE).json
