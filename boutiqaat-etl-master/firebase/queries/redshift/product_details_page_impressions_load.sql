{% set FDATE = "{}-{}-{}".format(DATE[:4], DATE[4:6], DATE[6:]) %}

DROP TABLE IF EXISTS tmp_product_details_page_impressions;
CREATE TEMP TABLE tmp_product_details_page_impressions(
    app_version VARCHAR(10),
    event_name VARCHAR(50),
    session_id	INTEGER,
    ga_session_number	INTEGER,
    advertising_id VARCHAR(50),
    event_date	DATE,
    event_datetime	TIMESTAMP,
    page_id VARCHAR(50),
    catalog_page_type VARCHAR(30),
    sku VARCHAR(250)
);

copy tmp_product_details_page_impressions from '{{S3PATH}}'
iam_role 'arn:aws:iam::652586300051:role/Redshift-athena-S3-readonly'
delimiter ','
region 'eu-west-1' 
CSV 
GZIP 
IGNOREHEADER 1 
emptyasnull
blanksasnull;


UNLOAD ('select * from tmp_product_details_page_impressions')
TO 's3://btq-etl/firebase/pdp_impressions/parquet/'
iam_role 'arn:aws:iam::652586300051:role/redshift-s3-writeAccess'
FORMAT AS PARQUET
PARTITION BY (event_date)
ALLOWOVERWRITE
MAXFILESIZE AS 512 MB;

alter table spectrum.firebase_product_details_page_impressions
add if not exists partition (event_date='{{FDATE}}')
location 's3://btq-etl/firebase/pdp_impressions/parquet/event_date={{FDATE}}';

DROP TABLE tmp_product_details_page_impressions;