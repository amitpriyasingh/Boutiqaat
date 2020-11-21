{% set FDATE = "{}-{}-{}".format(DATE[:4], DATE[4:6], DATE[6:]) %}

DROP TABLE IF EXISTS tmp_fact_impressions;
CREATE TEMP TABLE tmp_fact_impressions(
    event_name  VARCHAR(50),
    event_date VARCHAR(10),
    event_timestamp BIGINT,
    user_pseudo_id VARCHAR(50),
    advertising_id VARCHAR(50),
    app_version VARCHAR(10),
    ga_session_id INTEGER,
    ga_session_number INTEGER,
    item_list VARCHAR(200),
    page_id VARCHAR(200),
    catalog_page_type VARCHAR(200),
    sku VARCHAR(50)
);

copy tmp_fact_impressions from '{{S3PATH}}'
iam_role 'arn:aws:iam::652586300051:role/Redshift-athena-S3-readonly'
delimiter ','
region 'eu-west-1' 
CSV 
GZIP 
IGNOREHEADER 1 
emptyasnull
blanksasnull;

UNLOAD ('select 
	event_name, 
	DATE(event_date) as event_date, 
	(TIMESTAMP ''epoch'' + CAST(event_timestamp AS BIGINT)/1000000 *INTERVAL ''1 second'') as event_timestamp,
	user_pseudo_id,
	advertising_id,
	app_version,
	ga_session_id,
	ga_session_number,
	item_list,
	page_id,
	catalog_page_type,
	sku
from tmp_fact_impressions')
TO 's3://btq-etl/firebase/fact_impressions/parquet/'
iam_role 'arn:aws:iam::652586300051:role/redshift-s3-writeAccess'
FORMAT AS PARQUET
PARTITION BY (event_date)
ALLOWOVERWRITE
MAXFILESIZE AS 500 MB;

alter table spectrum.firebase_fact_impressions
add if not exists partition (event_date='{{FDATE}}')
location 's3://btq-etl/firebase/fact_impressions/parquet/event_date={{FDATE}}';

DROP TABLE tmp_fact_impressions;