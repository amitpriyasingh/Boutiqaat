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
NULL AS 'null'
FILLRECORD
TRUNCATECOLUMNS;

DELETE from OFS.order_batch_details WHERE DATE(inserted_on) = DATE('{{DATE}}');
INSERT INTO OFS.order_batch_details
SELECT web_order_no, reference_order_no, item_id, batch_id, order_type, delivery_type, pick_location, vendor_id, sent_for_pick, total_item_count, is_surface, is_foc, order_category, packaging_location, item_no, error, ready_for_archive, inserted_on, inserted_by, updated_on, updated_by
FROM tmp_order_batch_details;
COMMIT;