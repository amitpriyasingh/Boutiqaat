BEGIN;
DROP TABLE IF EXISTS tmp_bi_celebrity_master;
CREATE TEMP TABLE tmp_bi_celebrity_master(
    entry_id BIGINT,
    celebrity_id INTEGER,
    celebrity_name VARCHAR(100),
    current_am VARCHAR(100),
    code VARCHAR(10),
    am_email VARCHAR(100),
    rm_name VARCHAR(100),
    rm_email VARCHAR(100),
    am_effective_date TIMESTAMP,
    am_mapping_end_date TIMESTAMP,
    is_entry_active SMALLINT,
    last_updated_utc TIMESTAMP NOT NULL,
    boutique_gender INTEGER
);

copy tmp_bi_celebrity_master from '{{S3PATH}}'
iam_role 'arn:aws:iam::652586300051:role/Redshift-athena-S3-readonly'
delimiter '\t'
region 'eu-west-1' 
acceptinvchars
CSV 
GZIP 
emptyasnull
blanksasnull
NULL AS 'null';

CREATE TABLE IF NOT EXISTS aoi.bi_celebrity_master (
    entry_id BIGINT NOT NULL ENCODE LZO,
    celebrity_id INTEGER NOT NULL ENCODE LZO DISTKEY SORTKEY,
    celebrity_name VARCHAR(100) NULL ENCODE LZO,
    current_am VARCHAR(100) NULL ENCODE LZO,
    code VARCHAR(10) NULL ENCODE LZO,
    am_email VARCHAR(100) NULL ENCODE LZO,
    rm_name VARCHAR(100) NULL ENCODE LZO,
    rm_email VARCHAR(100) NULL ENCODE LZO,
    am_effective_date TIMESTAMP NOT NULL ENCODE LZO,
    am_mapping_end_date TIMESTAMP NULL ENCODE LZO,
    is_entry_active SMALLINT NOT NULL ENCODE DELTA,
    last_updated_utc TIMESTAMP NOT NULL ENCODE LZO,
    boutique_gender INTEGER NULL ENCODE LZO
);

DELETE FROM aoi.bi_celebrity_master WHERE 1=1;

INSERT INTO aoi.bi_celebrity_master
SELECT entry_id, celebrity_id, celebrity_name, current_am, code, am_email, rm_name, rm_email, am_effective_date, am_mapping_end_date, is_entry_active, last_updated_utc, boutique_gender
FROM tmp_bi_celebrity_master;


COMMIT;
