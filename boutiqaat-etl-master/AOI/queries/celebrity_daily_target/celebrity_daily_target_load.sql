BEGIN;
DROP TABLE IF EXISTS tmp_celebrity_daily_target;
CREATE TEMP TABLE tmp_celebrity_daily_target(
    target_date TIMESTAMP,
    celebrity_id INTEGER,
    celebrity_name VARCHAR(100),
    target_kwd DECIMAL(10,3)
);

copy tmp_celebrity_daily_target from '{{S3PATH}}'
iam_role 'arn:aws:iam::652586300051:role/Redshift-athena-S3-readonly'
delimiter '\t'
region 'eu-west-1' 
acceptinvchars
CSV 
GZIP 
emptyasnull
blanksasnull
NULL AS 'null';


CREATE TABLE IF NOT EXISTS aoi.celebrity_daily_target (
    target_date TIMESTAMP NULL,
    celebrity_id INTEGER NULL,
    celebrity_name VARCHAR(100) NULL,
    target_kwd DECIMAL(10,3) NULL
);

DELETE FROM aoi.celebrity_daily_target WHERE 1=1;

INSERT INTO aoi.celebrity_daily_target
SELECT target_date,celebrity_id,celebrity_name,target_kwd
FROM tmp_celebrity_daily_target;


COMMIT;

