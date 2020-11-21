BEGIN;
DROP TABLE IF EXISTS tmp_crm_orders;
CREATE TEMP TABLE tmp_crm_orders(
    id INTEGER,
    web_order_no VARCHAR(50),
    order_status INTEGER,
    order_datetime TIMESTAMP,
    payment_method VARCHAR(10),
    user_assign VARCHAR(45),
    order_category VARCHAR(45),
    first_name VARCHAR(200),
    middle_name VARCHAR(200),
    last_name VARCHAR(200),
    customer_id VARCHAR(45),
    customer_email VARCHAR(100),
    customer_phone VARCHAR(100),
    packaging_location INTEGER,
    inserted_on TIMESTAMP,
    inserted_by VARCHAR(45),
    updated_on TIMESTAMP,
    updated_by VARCHAR(45),
    ready_for_archive SMALLINT,
    country VARCHAR(100),
    order_source VARCHAR(50),
    order_amount DECIMAL(10,2),
    currency VARCHAR(45),
    order_type VARCHAR(20),
    reference_order_no VARCHAR(50),
    app_order_no VARCHAR(50),
    company VARCHAR(50),
    is_rule_run INTEGER,
    assignd_by_user_id INTEGER
);

copy tmp_crm_orders from '{{S3PATH}}'
iam_role 'arn:aws:iam::652586300051:role/Redshift-athena-S3-readonly'
delimiter '\t'
region 'eu-west-1' 
acceptinvchars
CSV 
GZIP 
emptyasnull
blanksasnull
NULL AS 'null';

CREATE TABLE IF NOT EXISTS OFS.crm_orders (
    id INTEGER NOT NULL ENCODE LZO DISTKEY SORTKEY PRIMARY KEY,
    web_order_no VARCHAR(50) NULL ENCODE LZO,
    order_status INTEGER NULL ENCODE LZO,
    order_datetime TIMESTAMP NULL ENCODE LZO,
    payment_method VARCHAR(10) NULL ENCODE LZO,
    user_assign VARCHAR(45) NULL ENCODE LZO,
    order_category VARCHAR(45) NULL ENCODE LZO,
    first_name VARCHAR(200) NULL ENCODE LZO,
    middle_name VARCHAR(200) NULL ENCODE LZO,
    last_name VARCHAR(200) NULL ENCODE LZO,
    customer_id VARCHAR(45) NULL ENCODE LZO,
    customer_email VARCHAR(100) NULL ENCODE LZO,
    customer_phone VARCHAR(100) NULL ENCODE LZO,
    packaging_location INTEGER NULL ENCODE LZO,
    inserted_on TIMESTAMP NOT NULL ENCODE LZO,
    inserted_by VARCHAR(45) NULL ENCODE LZO,
    updated_on TIMESTAMP NULL ENCODE LZO,
    updated_by VARCHAR(45) NULL ENCODE LZO,
    ready_for_archive SMALLINT NOT NULL ENCODE LZO,
    country VARCHAR(100) NULL ENCODE LZO,
    order_source VARCHAR(50) NULL ENCODE LZO,
    order_amount DECIMAL(10,2) NULL ENCODE LZO,
    currency VARCHAR(45) NULL ENCODE LZO,
    order_type VARCHAR(20) NULL ENCODE LZO,
    reference_order_no VARCHAR(50) NULL ENCODE LZO,
    app_order_no VARCHAR(50) NULL ENCODE LZO,
    company VARCHAR(50) NULL ENCODE LZO,
    is_rule_run INTEGER NOT NULL ENCODE LZO,
    assignd_by_user_id INTEGER NULL ENCODE LZO
);

DELETE FROM OFS.crm_orders WHERE 1=1;

INSERT INTO OFS.crm_orders
SELECT id, web_order_no, order_status, order_datetime, payment_method, user_assign, order_category, first_name, middle_name, last_name, customer_id, customer_email, customer_phone, packaging_location, inserted_on, inserted_by, updated_on, updated_by, ready_for_archive, country, order_source, order_amount, currency, order_type, reference_order_no, app_order_no, company, is_rule_run,assignd_by_user_id 
FROM tmp_crm_orders;


COMMIT;
