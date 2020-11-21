BEGIN;
DROP TABLE IF EXISTS tmp_status_master;
CREATE TEMP TABLE tmp_status_master(
    id INTEGER,
    status_name VARCHAR(50),
    ready_for_archive SMALLINT,
    inserted_by VARCHAR(100),
    inserted_on TIMESTAMP,
    updated_by VARCHAR(100),
    updated_on TIMESTAMP,
    is_cancelable SMALLINT,
    is_holdable SMALLINT,
    is_address_change SMALLINT,
    process_sequence INTEGER,
    next_process INTEGER,
    applicable_for_rule BOOLEAN,
    is_exchangeable SMALLINT,
    is_waiver_off_able SMALLINT,
    is_bulk_status_update BOOLEAN,
    is_order_editable BOOLEAN,
    sms_required BOOLEAN,
    is_returnable INTEGER,
    gwp_added SMALLINT,
    crm SMALLINT,
    ofs SMALLINT,
    is_assignable SMALLINT
);

copy tmp_status_master from '{{S3PATH}}'
iam_role 'arn:aws:iam::652586300051:role/Redshift-athena-S3-readonly'
delimiter '\t'
region 'eu-west-1' 
acceptinvchars
CSV 
GZIP 
emptyasnull
blanksasnull
NULL AS 'null';

CREATE TABLE IF NOT EXISTS OFS.status_master (
    id INTEGER NOT NULL ENCODE DELTA PRIMARY KEY,
    status_name VARCHAR(50) NULL ENCODE LZO,
    ready_for_archive SMALLINT NOT NULL ENCODE DELTA,
    inserted_by VARCHAR(100) NULL ENCODE LZO,
    inserted_on TIMESTAMP NULL ENCODE LZO,
    updated_by VARCHAR(100) NULL ENCODE LZO,
    updated_on TIMESTAMP NULL ENCODE LZO,
    is_cancelable SMALLINT NOT NULL ENCODE DELTA,
    is_holdable SMALLINT NOT NULL ENCODE DELTA,
    is_address_change SMALLINT NOT NULL ENCODE DELTA,
    process_sequence INTEGER NULL ENCODE LZO,
    next_process INTEGER NULL ENCODE LZO,
    applicable_for_rule BOOLEAN NOT NULL ENCODE ZSTD,
    is_exchangeable SMALLINT NOT NULL ENCODE DELTA,
    is_waiver_off_able SMALLINT NOT NULL ENCODE DELTA,
    is_bulk_status_update BOOLEAN NOT NULL ENCODE ZSTD,
    is_order_editable BOOLEAN NOT NULL ENCODE ZSTD,
    sms_required BOOLEAN NOT NULL ENCODE ZSTD,
    is_returnable INTEGER NULL ENCODE LZO,
    gwp_added SMALLINT NOT NULL ENCODE DELTA,
    crm SMALLINT NOT NULL ENCODE DELTA,
    ofs SMALLINT NOT NULL ENCODE DELTA,
    is_assignable SMALLINT NULL ENCODE DELTA
);

DELETE FROM OFS.status_master WHERE 1=1;

INSERT INTO OFS.status_master
SELECT id, status_name, ready_for_archive, inserted_by, inserted_on, updated_by, updated_on, is_cancelable, is_holdable, is_address_change, process_sequence, next_process, applicable_for_rule, is_exchangeable, is_waiver_off_able, is_bulk_status_update, is_order_editable, sms_required, is_returnable, gwp_added, crm, ofs, is_assignable
FROM tmp_status_master;


COMMIT;
