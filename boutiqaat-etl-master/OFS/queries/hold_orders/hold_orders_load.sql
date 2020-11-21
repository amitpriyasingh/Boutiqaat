BEGIN;
DROP TABLE IF EXISTS tmp_hold_orders;
CREATE TEMP TABLE tmp_hold_orders(
    id INTEGER,
    web_order_no VARCHAR(50),
    item_id INTEGER,
    reason VARCHAR(500),
    notes VARCHAR(250),
    user_id INTEGER,
    inserted_on TIMESTAMP,
    deleted SMALLINT,
    deleted_on TIMESTAMP,
    operation_type INTEGER,
    ready_for_archive SMALLINT,
    inserted_by VARCHAR(100),
    updated_on TIMESTAMP,
    updated_by VARCHAR(100)
);

copy tmp_hold_orders from '{{S3PATH}}'
iam_role 'arn:aws:iam::652586300051:role/Redshift-athena-S3-readonly'
delimiter '\t'
region 'eu-west-1' 
acceptinvchars
CSV 
GZIP 
emptyasnull
blanksasnull
NULL AS 'null';

CREATE TABLE IF NOT EXISTS OFS.hold_orders (
    id INTEGER NOT NULL ENCODE LZO SORTKEY DISTKEY PRIMARY KEY,
    web_order_no VARCHAR(50) NULL ENCODE LZO,
    item_id INTEGER NULL ENCODE LZO,
    reason VARCHAR(500) NULL ENCODE LZO,
    notes VARCHAR(250) NULL ENCODE LZO,
    user_id INTEGER NULL ENCODE LZO,
    inserted_on TIMESTAMP NULL ENCODE LZO,
    deleted SMALLINT NULL ENCODE DELTA,
    deleted_on TIMESTAMP NULL ENCODE LZO,
    operation_type INTEGER NULL ENCODE LZO,
    ready_for_archive SMALLINT NOT NULL ENCODE DELTA,
    inserted_by VARCHAR(100) NULL ENCODE LZO,
    updated_on TIMESTAMP NULL ENCODE LZO,
    updated_by VARCHAR(100) NULL ENCODE LZO
);

DELETE FROM OFS.hold_orders WHERE 1=1;

INSERT INTO OFS.hold_orders
SELECT id, web_order_no, item_id, reason, notes, user_id, inserted_on, deleted, deleted_on, operation_type, ready_for_archive, inserted_by, updated_on, updated_by
FROM tmp_hold_orders;


COMMIT;
