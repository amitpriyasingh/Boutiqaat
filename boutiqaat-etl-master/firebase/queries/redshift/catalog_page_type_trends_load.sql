DROP TABLE IF EXISTS tmp_catalog_page_type_trends;
CREATE TEMP TABLE tmp_catalog_page_type_trends(
    event_date VARCHAR(20),
    app_version VARCHAR(20),
    event_name VARCHAR(40),
    catalog_page_type VARCHAR(40),
    impressions_count INTEGER
);

copy tmp_catalog_page_type_trends from '{{S3PATH}}'
iam_role 'arn:aws:iam::652586300051:role/Redshift-athena-S3-readonly'
delimiter ','
region 'eu-west-1' 
CSV 
GZIP 
IGNOREHEADER 1 
emptyasnull
blanksasnull;

CREATE TABLE IF NOT EXISTS firebase.catalog_page_type_trends(
    event_date VARCHAR(20) ENCODE LZO SORTKEY,
    app_version VARCHAR(20) ENCODE LZO,
    event_name VARCHAR(40) ENCODE LZO,
    catalog_page_type VARCHAR(40) ENCODE LZO DISTKEY,
    impressions_count INTEGER
);

BEGIN;
DELETE from firebase.catalog_page_type_trends WHERE event_date = '{{DATE}}';
INSERT INTO firebase.catalog_page_type_trends 
select * from tmp_catalog_page_type_trends;
COMMIT;