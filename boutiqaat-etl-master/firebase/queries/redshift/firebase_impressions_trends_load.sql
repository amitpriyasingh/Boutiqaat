BEGIN;
DROP TABLE IF EXISTS tmp_impressions_trends;
CREATE TEMP TABLE tmp_impressions_trends(
    event_date DATE,
    store VARCHAR(20),
    store_id VARCHAR(20),
    country VARCHAR(20),
    platform VARCHAR(20),
    app_version VARCHAR(20),
    event_name VARCHAR(50),
    catalog_page_type VARCHAR(250),
    catalog_page_id VARCHAR(250),
    catalog_page_name VARCHAR(250),
    sku VARCHAR(250),
    impressions INTEGER
);

copy tmp_impressions_trends from '{{S3PATH}}'
iam_role 'arn:aws:iam::652586300051:role/Redshift-athena-S3-readonly'
delimiter ','
region 'eu-west-1' 
CSV 
GZIP 
IGNOREHEADER 1 
emptyasnull
blanksasnull;

CREATE TABLE IF NOT EXISTS firebase.impressions_trends(
    event_date DATE ENCODE LZO,
    store VARCHAR(20) ENCODE LZO,
    store_id VARCHAR(20) ENCODE LZO,
    country VARCHAR(20) ENCODE LZO,
    platform VARCHAR(20) ENCODE LZO,
    app_version VARCHAR(20) ENCODE LZO,
    event_name VARCHAR(50) ENCODE LZO,
    catalog_page_type VARCHAR(250) ENCODE LZO,
    catalog_page_id VARCHAR(250) ENCODE LZO,
    catalog_page_name VARCHAR(250) ENCODE LZO,
    sku VARCHAR(250) ENCODE LZO,
    impressions INTEGER ENCODE LZO
);

INSERT INTO firebase.impressions_trends 
select * from tmp_impressions_trends;
COMMIT;