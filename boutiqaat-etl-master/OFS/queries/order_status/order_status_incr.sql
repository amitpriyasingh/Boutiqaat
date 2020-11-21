BEGIN;
DROP TABLE IF EXISTS tmp_order_status;
CREATE TEMP TABLE tmp_order_status(
    id INTEGER,
    web_order_no VARCHAR(50),
    item_id INTEGER,
    item_no VARCHAR(50),
    status_id INTEGER,
    sku VARCHAR(50),
    ready_for_archive SMALLINT,
    inserted_by VARCHAR(100),
    inserted_on TIMESTAMP,
    updated_by VARCHAR(100),
    updated_on TIMESTAMP
);

copy tmp_order_status from '{{S3PATH}}'
iam_role 'arn:aws:iam::652586300051:role/Redshift-athena-S3-readonly'
delimiter '\t'
region 'eu-west-1' 
acceptinvchars
CSV 
GZIP 
emptyasnull
blanksasnull
NULL AS 'null';

INSERT INTO OFS.order_status
SELECT id, web_order_no, item_id, item_no, status_id, sku, ready_for_archive, inserted_by, inserted_on, updated_by, updated_on
FROM tmp_order_status;


COMMIT;
