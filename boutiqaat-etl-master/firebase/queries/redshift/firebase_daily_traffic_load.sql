DROP TABLE IF EXISTS tmp_daily_traffic;
CREATE TEMP TABLE tmp_daily_traffic(
    event_date_kwt TIMESTAMP,
    platform VARCHAR(20),
    app_version VARCHAR(20),
    country VARCHAR(20),
    page_type VARCHAR(20),
    page_id VARCHAR(20),
    page_name VARCHAR(100),
    total_uniq_users INTEGER,
    primary_uniq_users INTEGER,
    secondary_uniq_users INTEGER,
    total_page_loads INTEGER,
    primary_page_loads INTEGER,
    secondary_page_loads INTEGER
);

copy tmp_daily_traffic from '{{S3PATH}}'
iam_role 'arn:aws:iam::652586300051:role/Redshift-athena-S3-readonly'
delimiter ','
region 'eu-west-1' 
CSV 
GZIP 
IGNOREHEADER 1 
emptyasnull
blanksasnull;

CREATE TABLE IF NOT EXISTS firebase.daily_traffic(
    ingestion_date TIMESTAMP,
    event_date_kwt TIMESTAMP,
    platform VARCHAR(20),
    app_version VARCHAR(20),
    country VARCHAR(20),
    page_type VARCHAR(20),
    page_id VARCHAR(20),
    page_name VARCHAR(100),
    total_uniq_users INTEGER,
    primary_uniq_users INTEGER,
    secondary_uniq_users INTEGER,
    total_page_loads INTEGER,
    primary_page_loads INTEGER,
    secondary_page_loads INTEGER
);

BEGIN;
DELETE from firebase.daily_traffic WHERE ingestion_date = DATE('{{DATE}}');
INSERT INTO firebase.daily_traffic 
select 
    DATE('{{DATE}}') as ingestion_date, 
    event_date_kwt,
    platform,
    app_version,
    country,
    page_type,
    page_id,
    CASE WHEN page_type='Celebrity' THEN TRIM(REPLACE(LOWER(page_name), 'boutique', '')) 
        ELSE TRIM(page_name)
    END as page_name,
    total_uniq_users,
    primary_uniq_users,
    secondary_uniq_users,
    total_page_loads,
    primary_page_loads
from tmp_daily_traffic;

UPDATE firebase.daily_traffic
SET page_name = bcm.celebrity_name
FROM aoi.bi_celebrity_master bcm
WHERE page_type='Celebrity' and page_id not in (0,1) and page_id=bcm.celebrity_id;

COMMIT;