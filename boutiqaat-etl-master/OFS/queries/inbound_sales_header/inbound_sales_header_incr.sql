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


INSERT INTO OFS.inbound_sales_header
SELECT id, order_json_id, web_order_no, customer_id, payment_method_code, payment_gateway, order_datetime, order_type, frequency, priority, is_exchange, is_gift_wrap, country, ready_for_archive, inserted_on, inserted_by, updated_on, updated_by, order_category, confirm, reference_order_no, app_order_no, order_source, company, error_message, is_sync, retry_count
FROM tmp_inbound_sales_header;


COMMIT;
