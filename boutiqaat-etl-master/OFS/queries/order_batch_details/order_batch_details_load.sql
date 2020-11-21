BEGIN;
DROP TABLE IF EXISTS tmp_order_batch_details;
CREATE TEMP TABLE tmp_order_batch_details(
    web_order_no VARCHAR(50),
    reference_order_no VARCHAR(50),
    item_id INTEGER,
    batch_id INTEGER,
    order_type VARCHAR(20),
    delivery_type VARCHAR(20),
    pick_location VARCHAR(50),
    vendor_id INTEGER,
    sent_for_pick SMALLINT,
    total_item_count INTEGER,
    is_surface SMALLINT,
    is_foc BOOLEAN,
    order_category VARCHAR(100),
    packaging_location VARCHAR(50),
    item_no VARCHAR(20),
    error VARCHAR(500),
    ready_for_archive SMALLINT,
    inserted_on TIMESTAMP,
    inserted_by VARCHAR(100),
    updated_on TIMESTAMP,
    updated_by VARCHAR(100)
);

copy tmp_order_batch_details from '{{S3PATH}}'
iam_role 'arn:aws:iam::652586300051:role/Redshift-athena-S3-readonly'
delimiter '\t'
region 'eu-west-1' 
acceptinvchars
CSV 
GZIP 
emptyasnull
blanksasnull
NULL AS 'null';

CREATE TABLE IF NOT EXISTS OFS.order_batch_details (
    web_order_no VARCHAR(50) NULL ENCODE LZO,
    reference_order_no VARCHAR(50) NULL ENCODE LZO,
    item_id INTEGER NOT NULL ENCODE DELTA,
    batch_id INTEGER NOT NULL ENCODE DELTA,
    order_type VARCHAR(20) NULL ENCODE LZO,
    delivery_type VARCHAR(20) NULL ENCODE LZO,
    pick_location VARCHAR(50) NULL ENCODE LZO,
    vendor_id INTEGER NULL ENCODE DELTA,
    sent_for_pick SMALLINT NULL ENCODE DELTA,
    total_item_count INTEGER NULL ENCODE DELTA,
    is_surface SMALLINT NULL ENCODE DELTA,
    is_foc BOOLEAN NOT NULL ENCODE ZSTD,
    order_category VARCHAR(100) NULL ENCODE LZO,
    packaging_location VARCHAR(50) NULL ENCODE LZO,
    item_no VARCHAR(20) NULL ENCODE LZO,
    error VARCHAR(500) NULL ENCODE LZO,
    ready_for_archive SMALLINT NOT NULL ENCODE DELTA,
    inserted_on TIMESTAMP NOT NULL ENCODE LZO,
    inserted_by VARCHAR(100) NULL ENCODE LZO,
    updated_on TIMESTAMP NULL ENCODE LZO,
    updated_by VARCHAR(100) NULL ENCODE LZO
);

DELETE FROM OFS.order_batch_details WHERE 1=1;

INSERT INTO OFS.order_batch_details
SELECT web_order_no, reference_order_no, item_id, batch_id, order_type, delivery_type, pick_location, vendor_id, sent_for_pick, total_item_count, is_surface, is_foc, order_category, packaging_location, item_no, error, ready_for_archive, inserted_on, inserted_by, updated_on, updated_by
FROM tmp_order_batch_details;


COMMIT;