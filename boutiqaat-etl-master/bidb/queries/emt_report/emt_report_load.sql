BEGIN;
DROP TABLE IF EXISTS tmp_emt_report;
CREATE TEMP TABLE tmp_emt_report(
    event_id VARCHAR(255),
    events_header_id INTEGER,
    user_name VARCHAR(55),
    celebrity_name VARCHAR(55),
    celebrity_id INTEGER,
    generic VARCHAR(55),
    event_portal VARCHAR(50),
    event_type VARCHAR(50),
    event_class VARCHAR(50),
    bq_post INTEGER,
    total_post INTEGER,
    event_date TIMESTAMP,
    event_time TIMESTAMP,
    created_at TIMESTAMP,
    updated_at TIMESTAMP,
    event_hours INTEGER,
    event_minutes INTEGER,
    remark VARCHAR(1000),
    labelid INTEGER,
    skuid VARCHAR(64),
    product_entity_id INTEGER
);

copy tmp_emt_report from '{{S3PATH}}'
iam_role 'arn:aws:iam::652586300051:role/Redshift-athena-S3-readonly'
delimiter '\t'
region 'eu-west-1' 
acceptinvchars
CSV 
GZIP 
emptyasnull
blanksasnull
NULL AS 'null';

DROP TABLE IF EXISTS bidb.emt_report;
CREATE TABLE IF NOT EXISTS bidb.emt_report (
    event_id VARCHAR(255) NOT NULL ENCODE LZO DISTKEY,
    events_header_id INTEGER NOT NULL ENCODE LZO,
    user_name VARCHAR(55) NULL ENCODE LZO,
    celebrity_name VARCHAR(55) NULL ENCODE LZO,
    celebrity_id INTEGER NULL ENCODE LZO,
    generic VARCHAR(55) NULL ENCODE LZO,
    event_portal VARCHAR(50) NULL ENCODE LZO,
    event_type VARCHAR(50) NULL ENCODE LZO,
    event_class VARCHAR(50) NULL ENCODE LZO,
    bq_post INTEGER NOT NULL ENCODE LZO,
    total_post INTEGER NOT NULL ENCODE LZO,
    event_date TIMESTAMP NULL ENCODE LZO SORTKEY,
    event_time TIMESTAMP NULL ENCODE LZO,
    created_at TIMESTAMP NULL ENCODE LZO,
    updated_at TIMESTAMP NULL ENCODE LZO,
    event_hours INTEGER NULL ENCODE LZO,
    event_minutes INTEGER NULL ENCODE LZO,
    remark VARCHAR(1000) NULL ENCODE LZO,
    labelid INTEGER NULL ENCODE LZO,
    skuid VARCHAR(64) NULL ENCODE LZO,
    product_entity_id INTEGER NULL ENCODE LZO
);

INSERT INTO bidb.emt_report
SELECT event_id,events_header_id,user_name,celebrity_name,celebrity_id,generic,event_portal,event_type,event_class,bq_post,total_post,event_date,event_time,created_at,updated_at,event_hours,event_minutes,remark,labelid,skuid,product_entity_id
FROM tmp_emt_report;
COMMIT;