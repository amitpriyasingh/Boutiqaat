BEGIN;
DROP TABLE IF EXISTS tmp_cancelled_orders;
CREATE TEMP TABLE tmp_cancelled_orders(
    id INTEGER,
    item_id INTEGER,
    inserted_on TIMESTAMP,
    cancelled_date TIMESTAMP,
    ready_for_archive SMALLINT,
    inserted_by VARCHAR(100),
    updated_on TIMESTAMP,
    updated_by VARCHAR(100),
    reason VARCHAR(50),
    notes VARCHAR(250),
    source_system VARCHAR(20),
    item_no VARCHAR(50),
    sync BOOLEAN,
    error_message VARCHAR(250),
    retry_count INTEGER,
    is_sync_for_wms BOOLEAN,
    sync_datetime_for_wms TIMESTAMP,
    retry_count_for_wms INTEGER,
    error_message_for_wms VARCHAR(500),
    cancel_type VARCHAR(15),
    is_sync_for_nav BOOLEAN,
    sync_datetime_for_nav TIMESTAMP,
    retry_count_for_nav INTEGER,
    error_message_for_nav VARCHAR(1000),
    web_order_no VARCHAR(50)
);

copy tmp_cancelled_orders from '{{S3PATH}}'
iam_role 'arn:aws:iam::652586300051:role/Redshift-athena-S3-readonly'
delimiter '\t'
region 'eu-west-1' 
acceptinvchars
CSV 
GZIP 
emptyasnull
blanksasnull
NULL AS 'null';

CREATE TABLE IF NOT EXISTS OFS.cancelled_orders (
    id INTEGER NOT NULL ENCODE LZO DISTKEY SORTKEY PRIMARY KEY,
    item_id INTEGER NULL ENCODE LZO,
    inserted_on TIMESTAMP NOT NULL ENCODE LZO,
    cancelled_date TIMESTAMP NOT NULL ENCODE LZO,
    ready_for_archive SMALLINT NOT NULL ENCODE DELTA,
    inserted_by VARCHAR(100) NULL ENCODE LZO,
    updated_on TIMESTAMP NULL ENCODE LZO,
    updated_by VARCHAR(100) NULL ENCODE LZO,
    reason VARCHAR(50) NULL ENCODE LZO,
    notes VARCHAR(250) NULL ENCODE LZO,
    source_system VARCHAR(20) NULL ENCODE LZO,
    item_no VARCHAR(50) NULL ENCODE LZO,
    sync BOOLEAN NOT NULL ENCODE ZSTD,
    error_message VARCHAR(250) NULL ENCODE LZO,
    retry_count INTEGER NOT NULL ENCODE LZO,
    is_sync_for_wms BOOLEAN NOT NULL ENCODE ZSTD,
    sync_datetime_for_wms TIMESTAMP NULL ENCODE LZO,
    retry_count_for_wms INTEGER NOT NULL ENCODE LZO,
    error_message_for_wms VARCHAR(500) NULL ENCODE LZO,
    cancel_type VARCHAR(15) NOT NULL ENCODE LZO,
    is_sync_for_nav BOOLEAN NOT NULL ENCODE ZSTD,
    sync_datetime_for_nav TIMESTAMP NULL ENCODE LZO,
    retry_count_for_nav INTEGER NOT NULL ENCODE LZO,
    error_message_for_nav VARCHAR(1000) NULL ENCODE LZO,
    web_order_no VARCHAR(50) NULL ENCODE LZO
);

DELETE FROM OFS.cancelled_orders WHERE 1=1;

INSERT INTO OFS.cancelled_orders
SELECT id, item_id, inserted_on, cancelled_date, ready_for_archive, inserted_by, updated_on, updated_by, reason, notes, source_system, item_no, sync, error_message, retry_count, is_sync_for_wms, sync_datetime_for_wms, retry_count_for_wms, error_message_for_wms, cancel_type, is_sync_for_nav, sync_datetime_for_nav, retry_count_for_nav, error_message_for_nav, web_order_no
FROM tmp_cancelled_orders;


COMMIT;

