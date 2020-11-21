BEGIN;
DROP TABLE IF EXISTS tmp_inbound_sales_header;
CREATE TEMP TABLE tmp_inbound_sales_header(
    id INTEGER,
    order_json_id INTEGER,
    web_order_no VARCHAR(50),
    customer_id VARCHAR(30),
    payment_method_code VARCHAR(50),
    payment_gateway VARCHAR(50),
    order_datetime TIMESTAMP,
    order_type VARCHAR(20),
    frequency SMALLINT,
    priority SMALLINT,
    is_exchange SMALLINT,
    is_gift_wrap SMALLINT,
    country VARCHAR(100),
    ready_for_archive SMALLINT,
    inserted_on TIMESTAMP,
    inserted_by VARCHAR(100),
    updated_on TIMESTAMP,
    updated_by VARCHAR(100),
    order_category VARCHAR(50),
    confirm SMALLINT,
    reference_order_no VARCHAR(50),
    app_order_no VARCHAR(50),
    order_source VARCHAR(50),
    company VARCHAR(45),
    error_message VARCHAR(100),
    is_sync INTEGER,
    retry_count INTEGER);

copy tmp_inbound_sales_header from '{{S3PATH}}'
iam_role 'arn:aws:iam::652586300051:role/Redshift-athena-S3-readonly'
delimiter '\t'
region 'eu-west-1' 
acceptinvchars
CSV 
GZIP 
emptyasnull
blanksasnull
NULL AS 'null';

CREATE TABLE IF NOT EXISTS OFS.inbound_sales_header (
    id INTEGER NOT NULL ENCODE LZO DISTKEY SORTKEY PRIMARY KEY,
    order_json_id INTEGER NOT NULL ENCODE LZO,
    web_order_no VARCHAR(50) NULL ENCODE LZO,
    customer_id VARCHAR(30) NULL ENCODE LZO,
    payment_method_code VARCHAR(50) NULL ENCODE LZO,
    payment_gateway VARCHAR(50) NULL ENCODE LZO,
    order_datetime TIMESTAMP NULL ENCODE LZO,
    order_type VARCHAR(20) NULL ENCODE LZO,
    frequency SMALLINT NULL ENCODE DELTA,
    priority SMALLINT NULL ENCODE DELTA,
    is_exchange SMALLINT NULL ENCODE DELTA,
    is_gift_wrap SMALLINT NULL ENCODE DELTA,
    country VARCHAR(100) NULL ENCODE LZO,
    ready_for_archive SMALLINT NOT NULL ENCODE DELTA,
    inserted_on TIMESTAMP NOT NULL ENCODE LZO,
    inserted_by VARCHAR(100) NULL ENCODE LZO,
    updated_on TIMESTAMP NULL ENCODE LZO,
    updated_by VARCHAR(100) NULL ENCODE LZO,
    order_category VARCHAR(50) NULL ENCODE LZO,
    confirm SMALLINT NOT NULL ENCODE DELTA,
    reference_order_no VARCHAR(50) NULL ENCODE LZO,
    app_order_no VARCHAR(50) NULL ENCODE LZO,
    order_source VARCHAR(50) NULL ENCODE LZO,
    company VARCHAR(45) NULL ENCODE LZO,
    error_message VARCHAR(100) NULL ENCODE LZO,
    is_sync INTEGER NOT NULL ENCODE LZO,
    retry_count INTEGER NOT NULL ENCODE LZO
);

DELETE FROM OFS.inbound_sales_header WHERE 1=1;

INSERT INTO OFS.inbound_sales_header
SELECT id, order_json_id, web_order_no, customer_id, payment_method_code, payment_gateway, order_datetime, order_type, frequency, priority, is_exchange, is_gift_wrap, country, ready_for_archive, inserted_on, inserted_by, updated_on, updated_by, order_category, confirm, reference_order_no, app_order_no, order_source, company, error_message, is_sync, retry_count
FROM tmp_inbound_sales_header;


COMMIT;
