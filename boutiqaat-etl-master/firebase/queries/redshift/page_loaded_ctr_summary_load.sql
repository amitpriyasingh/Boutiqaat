BEGIN;
DROP TABLE IF EXISTS tmp_page_loaded_ctr_summary;
CREATE TEMP TABLE tmp_page_loaded_ctr_summary(
    event_date_kwt TIMESTAMP,
    platform VARCHAR(50),
    app_version VARCHAR(50),
    plp_page VARCHAR(50),
    product_id INTEGER,
    impressions INTEGER,
    clicks INTEGER,
    ctr DECIMAL(38,20),
    impression_uniq_visitors INTEGER,
    click_uniq_visitors INTEGER,
    ctr_uniq_visitors DECIMAL(38,20)
);

copy tmp_page_loaded_ctr_summary from '{{S3PATH}}'
iam_role 'arn:aws:iam::652586300051:role/Redshift-athena-S3-readonly'
delimiter ','
region 'eu-west-1' 
CSV 
GZIP 
IGNOREHEADER 1 
emptyasnull
blanksasnull;

CREATE TABLE IF NOT EXISTS firebase.page_loaded_ctr_summary(
    event_date_kwt TIMESTAMP ENCODE LZO,
    platform VARCHAR(50) ENCODE LZO,
    app_version VARCHAR(50) ENCODE LZO,
    plp_page VARCHAR(50) ENCODE LZO,
    product_id INTEGER ENCODE LZO,
    impressions INTEGER ENCODE LZO,
    clicks INTEGER ENCODE LZO,
    ctr DECIMAL(38,20) ENCODE LZO,
    impression_uniq_visitors INTEGER ENCODE LZO,
    click_uniq_visitors INTEGER ENCODE LZO,
    ctr_uniq_visitors DECIMAL(38,20) ENCODE LZO,
    sync_date TIMESTAMP ENCODE LZO
);

DELETE FROM firebase.page_loaded_ctr_summary WHERE 1=1;

INSERT INTO firebase.page_loaded_ctr_summary 
select *, GETDATE() as sync_date from tmp_page_loaded_ctr_summary;
COMMIT;

