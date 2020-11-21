SHELL := /bin/bash
export PATH := /app/google-cloud-sdk/bin:$(PATH)

FIREBASE_GCLOUD=gs://btq_marketing_data/firebase
FIREBASE_EVENTS_TABLE=boutiqaat-online-shopping.analytics_151427213.events
DEFAULT_PROPERTIES := --replace --allow_large_results --max_rows 0 --nouse_cache --nouse_legacy_sql

ifndef DATES
override DATES=$(shell date --date="1 day ago" +%Y%m%d)-$(shell date --date="1 day ago" +%Y%m%d)
endif

# running commands with a range of dates
daterun:
	python3 -m pybashutil.yearmonthdayrun $(DATES) make do_$(TABLE) DATE='__DATE__' MONTH='__MONTH__' YEAR='__YEAR__'

create_partitioned_table: 
	bq mk --project_id boutiqaat-online-shopping --time_partitioning_type=DAY marketing.$(TABLE)

daily: generate_impressions daily_traffic generate_marketing_users page_loaded_ctr


#firebase data
generate_impressions:
	$(MAKE) daterun TABLE=generate_impressions

do_generate_impressions:
	$(MAKE) firebase_impressions
	$(MAKE) firebase_impressions_trends
	#$(MAKE) load_firebase_impressions
	#$(MAKE) firebase_pdp_impressions
	#$(MAKE) firebase_item_listing_impressions
	#$(MAKE) load_firebase_item_listing_impressions
	#$(MAKE) load_firebase_pdp_impressions

firebase_impressions:
	cat queries/bigquery/firebase_impressions.sql | env TABLE=$(FIREBASE_EVENTS_TABLE)_$(DATE) python3 -m pybashutil.render | bq query --destination_table 'boutiqaat-online-shopping:firebase.fact_impressions$$$(DATE)' $(DEFAULT_PROPERTIES)
	bq extract --project_id boutiqaat-online-shopping --destination_format CSV --compression GZIP 'boutiqaat-online-shopping:firebase.fact_impressions$$$(DATE)' gs://btq-omar-khairat/firebase/fact_impressions/$(DATE)/fact_impressionsـ$(DATE)_*.csv.gz
	gsutil -m rsync -d gs://btq-omar-khairat/firebase/fact_impressions/$(DATE)/ s3://btq-etl/firebase/fact_impressions/csv/$(DATE)/

firebase_impressions_trends:
	cat queries/bigquery/firebase_impressions_trends.sql | env DATE=$(DATE) python3 -m pybashutil.render | bq query --destination_table 'boutiqaat-online-shopping:firebase.firebase_impressions_trends$$$(DATE)' $(DEFAULT_PROPERTIES)
	bq extract --project_id boutiqaat-online-shopping --destination_format CSV --compression GZIP 'boutiqaat-online-shopping:firebase.firebase_impressions_trends$$$(DATE)' gs://btq-omar-khairat/firebase/firebase_impressions_trends/$(DATE)/firebase_impressions_trends_$(DATE)_*.csv.gz
	gsutil -m rsync -d gs://btq-omar-khairat/firebase/firebase_impressions_trends/$(DATE)/ s3://btq-etl/firebase/firebase_impressions_trends/csv/$(DATE)/
	cat queries/redshift/firebase_impressions_trends_load.sql | env S3PATH=s3://btq-etl/firebase/firebase_impressions_trends/csv/$(DATE)/ python3 -m pybashutil.render | redshift

load_firebase_impressions:
	cat queries/redshift/fact_impressions_load.sql | env S3PATH=s3://btq-etl/firebase/fact_impressions/csv/$(DATE)/ DATE=$(DATE) python3 -m pybashutil.render | redshift

firebase_pdp_impressions:
	cat queries/bigquery/product_details_page_impressions.sql | env DATE=$(DATE)  python3 -m pybashutil.render | bq query --destination_table 'boutiqaat-online-shopping:firebase.product_details_page_impressions$$$(DATE)' $(DEFAULT_PROPERTIES)
	bq extract --project_id boutiqaat-online-shopping --destination_format CSV --compression GZIP 'boutiqaat-online-shopping:firebase.product_details_page_impressions$$$(DATE)' gs://btq-omar-khairat/firebase/pdp_impressions/$(DATE)/firebase_pdp_impressions_$(DATE)_*.csv.gz
	gsutil -m rsync -d gs://btq-omar-khairat/firebase/pdp_impressions/$(DATE)/ s3://btq-etl/firebase/pdp_impressions/csv/$(DATE)/

load_firebase_pdp_impressions:
	cat queries/redshift/product_details_page_impressions_load.sql | env S3PATH=s3://btq-etl/firebase/pdp_impressions/csv/$(DATE)/ DATE=$(DATE) python3 -m pybashutil.render | redshift

firebase_item_listing_impressions:
	cat queries/bigquery/item_listing_impressions.sql | env DATE=$(DATE) python3 -m pybashutil.render | bq query --destination_table 'boutiqaat-online-shopping:firebase.item_listing_impressions$$$(DATE)' $(DEFAULT_PROPERTIES)
	bq extract --project_id boutiqaat-online-shopping --destination_format CSV --compression GZIP 'boutiqaat-online-shopping:firebase.item_listing_impressions$$$(DATE)' gs://btq-omar-khairat/firebase/item_listing_impressions/$(DATE)/firebase_item_listing_impressions_$(DATE)_*.csv.gz
	gsutil -m rsync -d gs://btq-omar-khairat/firebase/item_listing_impressions/$(DATE)/ s3://btq-etl/firebase/item_listing_impressions/csv/$(DATE)/

load_firebase_item_listing_impressions:
	cat queries/redshift/item_listing_impressions_load.sql | env S3PATH=s3://btq-etl/firebase/item_listing_impressions/csv/$(DATE)/ DATE=$(DATE) python3 -m pybashutil.render | redshift

#marketing data
generate_marketing_users:
	$(MAKE) daterun TABLE=generate_marketing_users
	$(MAKE) sessions_users_aggregated
	$(MAKE) load_sessions_users_aggregated

do_generate_marketing_users:
	$(MAKE) sessions_users_mapping

sessions_users_mapping:
	cat queries/bigquery/sessions_users_mapping.sql | env TABLE=$(FIREBASE_EVENTS_TABLE)_$(DATE) python3 -m pybashutil.render | bq query --destination_table 'boutiqaat-online-shopping:firebase.sessions_users$$$(DATE)' $(DEFAULT_PROPERTIES)
	
sessions_users_aggregated:
	cat queries/bigquery/sessions_users_aggregated.sql | bq query --destination_table 'boutiqaat-online-shopping:firebase.sessions_users_aggregated' $(DEFAULT_PROPERTIES)
	bq extract --project_id boutiqaat-online-shopping --destination_format CSV --compression GZIP 'boutiqaat-online-shopping:firebase.sessions_users_aggregated' gs://btq-omar-khairat/firebase/sessions_users_aggregated/sessions_users_aggregated.csv.gz
	gsutil -m rsync -d gs://btq-omar-khairat/firebase/sessions_users_aggregated/ s3://btq-etl/firebase/sessions_users_aggregated/csv/
	
load_sessions_users_aggregated:
	cat queries/redshift/sessions_users_aggregated_load.sql | env S3PATH=s3://btq-etl/firebase/sessions_users_aggregated/csv/ python3 -m pybashutil.render | redshift


#firebase impressions trends
generate_impressions_trends:
	$(MAKE) daterun TABLE=generate_impressions_trends

do_generate_impressions_trends:
	$(MAKE) catalog_page_type_trends
	$(MAKE) catalog_page_type_trends_transfer
	$(MAKE) catalog_page_type_trends_load

catalog_page_type_trends:
	cat queries/bigquery/catalog_page_type_trends.sql | env TABLE=$(FIREBASE_EVENTS_TABLE)_$(DATE) python3 -m pybashutil.render | bq query --destination_table 'boutiqaat-online-shopping:firebase.catalog_page_type_trends$$$(DATE)' $(DEFAULT_PROPERTIES)

catalog_page_type_trends_transfer:
	cp /credentials/aws_credentials ~/.boto
	bq extract --project_id boutiqaat-online-shopping --destination_format CSV --compression GZIP 'boutiqaat-online-shopping:firebase.catalog_page_type_trends$$$(DATE)' gs://btq-omar-khairat/firebase/catalog_page_type_trends/$(DATE)/catalog_page_type_trends_$(DATE).csv.gz
	gsutil -m rsync -d gs://btq-omar-khairat/firebase/catalog_page_type_trends/$(DATE)/ s3://btq-etl/firebase/catalog_page_type_trends/csv/$(DATE)/

catalog_page_type_trends_load:
	cat queries/redshift/catalog_page_type_trends_load.sql | env S3PATH=s3://btq-etl/firebase/catalog_page_type_trends/csv/$(DATE)/ DATE=$(DATE) python3 -m pybashutil.render | redshift

# firebase daily traffic
daily_traffic:
	$(MAKE) daterun TABLE=daily_traffic

do_daily_traffic:
	$(MAKE) firebase_daily_traffic
	$(MAKE) firebase_daily_traffic_extract
	$(MAKE) firebase_daily_traffic_load

firebase_daily_traffic:
	cat queries/bigquery/firebase_daily_traffic.sql | env TABLE=$(FIREBASE_EVENTS_TABLE)_$(DATE) python3 -m pybashutil.render | bq query --destination_table 'boutiqaat-online-shopping:firebase.daily_traffic$$$(DATE)' $(DEFAULT_PROPERTIES)
	cat queries/bigquery/firebase_daily_traffic_update.sql | env DATE=$(DATE) python3 -m pybashutil.render | bq query --nouse_legacy_sql
	cat queries/bigquery/firebase_daily_traffic_overall.sql | env TABLE=$(FIREBASE_EVENTS_TABLE)_$(DATE) python3 -m pybashutil.render | bq query --destination_table 'boutiqaat-online-shopping:firebase.daily_traffic$$$(DATE)' --append --allow_large_results --max_rows 0 --nouse_cache --nouse_legacy_sql
	cat queries/bigquery/firebase_daily_traffic_mainsite.sql | env TABLE=$(FIREBASE_EVENTS_TABLE)_$(DATE) python3 -m pybashutil.render | bq query --destination_table 'boutiqaat-online-shopping:firebase.daily_traffic$$$(DATE)' --append --allow_large_results --max_rows 0 --nouse_cache --nouse_legacy_sql

firebase_daily_traffic_extract:
	bq extract --project_id boutiqaat-online-shopping --destination_format CSV --compression GZIP 'boutiqaat-online-shopping:firebase.daily_traffic$$$(DATE)' gs://btq-omar-khairat/firebase/daily_traffic/$(DATE)/firebase_daily_traffic_$(DATE).csv.gz
	gsutil -m rsync -d gs://btq-omar-khairat/firebase/daily_traffic/$(DATE)/ s3://btq-etl/firebase/daily_traffic/csv/$(DATE)/

firebase_daily_traffic_load:
	cat queries/redshift/firebase_daily_traffic_load.sql | env S3PATH=s3://btq-etl/firebase/daily_traffic/csv/$(DATE)/ DATE=$(DATE) python3 -m pybashutil.render | redshift

# firebase daily page loaded clicks
page_loaded_ctr:
	$(MAKE) daterun TABLE=page_loaded_ctr
	$(MAKE) page_loaded_ctr_summary
	$(MAKE) page_loaded_ctr_summary_extract
	$(MAKE) page_loaded_ctr_summary_load

do_page_loaded_ctr:
	$(MAKE) imp_page_loaded_clicks

imp_page_loaded_clicks:
	cat queries/bigquery/imp_page_loaded_clk_data.sql | env TABLE=$(FIREBASE_EVENTS_TABLE)_$(DATE) python3 -m pybashutil.render | bq query --destination_table 'boutiqaat-online-shopping:marketing.imp_page_loaded_clk_data$$$(DATE)' $(DEFAULT_PROPERTIES)

page_loaded_ctr_summary:
	cat queries/bigquery/page_loaded_ctr_summary.sql | bq query --destination_table 'boutiqaat-online-shopping:marketing.page_loaded_ctr_summary' $(DEFAULT_PROPERTIES)

page_loaded_ctr_summary_extract:
	bq extract --project_id boutiqaat-online-shopping --destination_format CSV --compression GZIP 'boutiqaat-online-shopping:marketing.page_loaded_ctr_summary' gs://btq-omar-khairat/firebase/page_loaded_ctr_summary/page_loaded_ctr_summary_*.csv.gz
	gsutil -m rsync -d gs://btq-omar-khairat/firebase/page_loaded_ctr_summary/ s3://btq-etl/firebase/page_loaded_ctr_summary/csv/

page_loaded_ctr_summary_load:
	cat queries/redshift/page_loaded_ctr_summary_load.sql | env S3PATH=s3://btq-etl/firebase/page_loaded_ctr_summary/csv/ python3 -m pybashutil.render | redshift
