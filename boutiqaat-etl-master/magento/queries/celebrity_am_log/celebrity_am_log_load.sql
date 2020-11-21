BEGIN;
DROP TABLE IF EXISTS tmp_celebrity_am_log;
CREATE TEMP TABLE tmp_celebrity_am_log(
    id INTEGER,
    celebrity_id INTEGER,
    celebrity_name VARCHAR(255),
    am_id INTEGER,
    am_username VARCHAR(255),
    am_name VARCHAR(255),
    am_email VARCHAR(255),
    celebrity_am_startdate TIMESTAMP,
    celebrity_am_enddate TIMESTAMP,
    is_active SMALLINT,
    am_rm_name VARCHAR(255),
    am_rm_email VARCHAR(255)
);

copy tmp_celebrity_am_log from '{{S3PATH}}'
iam_role 'arn:aws:iam::652586300051:role/Redshift-athena-S3-readonly'
delimiter '\t'
region 'eu-west-1' 
acceptinvchars
CSV 
GZIP 
emptyasnull
blanksasnull
NULL AS 'null';

DROP TABLE IF EXISTS magento.celebrity_am_log;
CREATE TABLE magento.celebrity_am_log (
    id INTEGER NOT NULL ENCODE ZSTD SORTKEY PRIMARY KEY,
    celebrity_id INTEGER NOT NULL ENCODE ZSTD,
    celebrity_name VARCHAR(255) NOT NULL ENCODE ZSTD,
    am_id INTEGER NOT NULL ENCODE ZSTD DISTKEY,
    am_username VARCHAR(255) NOT NULL ENCODE ZSTD,
    am_name VARCHAR(255) NOT NULL ENCODE ZSTD,
    am_email VARCHAR(255) NOT NULL ENCODE ZSTD,
    celebrity_am_startdate TIMESTAMP NULL ENCODE ZSTD,
    celebrity_am_enddate TIMESTAMP NULL ENCODE ZSTD,
    is_active SMALLINT NOT NULL ENCODE ZSTD,
    am_rm_name VARCHAR(255) NULL ENCODE ZSTD,
    am_rm_email VARCHAR(255) NULL ENCODE ZSTD
);

INSERT INTO magento.celebrity_am_log
SELECT id,celebrity_id,celebrity_name,am_id,am_username,am_name,am_email,celebrity_am_startdate,celebrity_am_enddate,is_active,am_rm_name,am_rm_email
FROM tmp_celebrity_am_log;
COMMIT;
