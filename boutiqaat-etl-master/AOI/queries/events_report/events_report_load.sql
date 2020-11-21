BEGIN;
DROP TABLE IF EXISTS tmp_events_report;
CREATE TEMP TABLE tmp_events_report(
    id INTEGER,
    event_id VARCHAR(100),
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
    remark VARCHAR(max),
    labelid VARCHAR(50),
    productid VARCHAR(100),
    category1 VARCHAR(100),
    category2 VARCHAR(100),
    category3 VARCHAR(100),
    category4 VARCHAR(100),
    brand VARCHAR(50),
    purchase_type VARCHAR(50),
    account_manager VARCHAR(50),
    sku_name VARCHAR(255),
    sku_code VARCHAR(255),
    sku_code_name VARCHAR(255),
    alloc_bq_post DECIMAL(10,4)
);

copy tmp_events_report from '{{S3PATH}}'
iam_role 'arn:aws:iam::652586300051:role/Redshift-athena-S3-readonly'
delimiter '\t'
region 'eu-west-1' 
acceptinvchars
CSV 
GZIP 
emptyasnull
blanksasnull
NULL AS 'null';

CREATE TABLE IF NOT EXISTS aoi.events_report (
    id INTEGER NOT NULL ENCODE LZO PRIMARY KEY,
    event_id VARCHAR(100) NULL ENCODE LZO,
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
    event_date TIMESTAMP NOT NULL ENCODE LZO,
    event_time TIMESTAMP NULL ENCODE LZO,
    created_at TIMESTAMP NULL ENCODE LZO SORTKEY DISTKEY,
    updated_at TIMESTAMP NULL ENCODE LZO,
    event_hours INTEGER NULL ENCODE LZO,
    event_minutes INTEGER NULL ENCODE LZO,
    remark VARCHAR(max) NULL ENCODE LZO,
    labelid VARCHAR(50) NULL ENCODE LZO,
    productid VARCHAR(100) NULL ENCODE LZO,
    category1 VARCHAR(100) NULL ENCODE LZO,
    category2 VARCHAR(100) NULL ENCODE LZO,
    category3 VARCHAR(100) NULL ENCODE LZO,
    category4 VARCHAR(100) NULL ENCODE LZO,
    brand VARCHAR(50) NULL ENCODE LZO,
    purchase_type VARCHAR(50) NULL ENCODE LZO,
    account_manager VARCHAR(50) NULL ENCODE LZO,
    sku_name VARCHAR(255) NULL ENCODE LZO,
    sku_code VARCHAR(255) NULL ENCODE LZO,
    sku_code_name VARCHAR(255) NULL ENCODE LZO,
    alloc_bq_post DECIMAL(10,4) NULL ENCODE LZO
);

DELETE FROM aoi.events_report WHERE 1=1;

INSERT INTO aoi.events_report
SELECT id, event_id, events_header_id, user_name, celebrity_name, celebrity_id, generic, event_portal, event_type, event_class, bq_post, total_post, event_date, event_time, created_at, updated_at, event_hours, event_minutes, remark, labelid, productid, category1, category2, category3, category4, brand, purchase_type, account_manager, sku_name, sku_code, sku_code_name, alloc_bq_post
FROM tmp_events_report;


COMMIT;