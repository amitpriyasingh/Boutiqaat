DROP TABLE IF EXISTS tmp_inbound_payment_line;
CREATE TEMP TABLE tmp_inbound_payment_line(
    id INTEGER,
    is_header SMALLINT,
    web_order_no VARCHAR(50),
    item_id INTEGER,
    payment_gateway VARCHAR(45),
    payment_method_code VARCHAR(50),
    cod_charges DECIMAL(10,3),
    shipping_charges DECIMAL(10,3),
    gift_charges DECIMAL(10,3),
    amount DECIMAL(10,3),
    tax DECIMAL(10,3),
    amount_incl_tax DECIMAL(10,3),
    discount DECIMAL(10,3),
    coupon_code VARCHAR(100),
    coupon_amount DECIMAL(10,3),
    customized_charges DECIMAL(10,3),
    boutiqaat_credit DECIMAL(10,3),
    other_charges DECIMAL(10,3),
    agent_code VARCHAR(100),
    agent_commission DECIMAL(10,3),
    order_charges_processed SMALLINT,
    mrp_price DECIMAL(10,3),
    unit_price DECIMAL(10,3),
    unit_price_including_tax DECIMAL(10,3),
    wallet_name VARCHAR(100),
    wallet_amount DECIMAL(10,3),
    transaction_id VARCHAR(100),
    tax_percentage INTEGER,
    currency_code VARCHAR(20),
    currency_factor DECIMAL(25,20),
    ready_for_archive SMALLINT,
    inserted_on TIMESTAMP,
    inserted_by VARCHAR(100),
    updated_on TIMESTAMP,
    updated_by VARCHAR(100),
    celebrity_order_sync SMALLINT,
    retry_counter INTEGER,
    error_message VARCHAR(1000),
    custom_duty DECIMAL(10,3),
    authorization_id VARCHAR(255)
);

copy tmp_inbound_payment_line from '{{S3PATH}}'
iam_role 'arn:aws:iam::652586300051:role/Redshift-athena-S3-readonly'
delimiter '\t'
region 'eu-west-1' 
acceptinvchars
CSV 
GZIP 
emptyasnull
blanksasnull
NULL AS 'null';

CREATE TABLE IF NOT EXISTS OFS.inbound_payment_line (
    id INTEGER NULL ENCODE DELTA DISTKEY SORTKEY PRIMARY KEY,
    is_header SMALLINT NULL ENCODE DELTA,
    web_order_no VARCHAR(50) NULL ENCODE LZO,
    item_id INTEGER NULL ENCODE LZO,
    payment_gateway VARCHAR(45) NULL ENCODE LZO,
    payment_method_code VARCHAR(50) NULL ENCODE LZO,
    cod_charges DECIMAL(10,3) NULL ENCODE LZO,
    shipping_charges DECIMAL(10,3) NULL ENCODE LZO,
    gift_charges DECIMAL(10,3) NULL ENCODE LZO,
    amount DECIMAL(10,3) NULL ENCODE LZO,
    tax DECIMAL(10,3) NULL ENCODE LZO,
    amount_incl_tax DECIMAL(10,3) NULL ENCODE LZO,
    discount DECIMAL(10,3) NULL ENCODE LZO,
    coupon_code VARCHAR(100) NULL ENCODE LZO,
    coupon_amount DECIMAL(10,3) NULL ENCODE LZO,
    customized_charges DECIMAL(10,3) NULL ENCODE LZO,
    boutiqaat_credit DECIMAL(10,3) NULL ENCODE LZO,
    other_charges DECIMAL(10,3) NULL ENCODE LZO,
    agent_code VARCHAR(100) NULL ENCODE LZO,
    agent_commission DECIMAL(10,3) NULL ENCODE LZO,
    order_charges_processed SMALLINT NULL ENCODE LZO,
    mrp_price DECIMAL(10,3) NULL ENCODE LZO,
    unit_price DECIMAL(10,3) NULL ENCODE LZO,
    unit_price_including_tax DECIMAL(10,3) NULL ENCODE LZO,
    wallet_name VARCHAR(100) NULL ENCODE LZO,
    wallet_amount DECIMAL(10,3) NULL ENCODE LZO,
    transaction_id VARCHAR(100) NULL ENCODE LZO,
    tax_percentage INTEGER NULL ENCODE DELTA,
    currency_code VARCHAR(20) NULL ENCODE LZO,
    currency_factor DECIMAL(25,20) NULL ENCODE LZO,
    ready_for_archive SMALLINT NULL ENCODE LZO,
    inserted_on TIMESTAMP NULL ENCODE LZO,
    inserted_by VARCHAR(100) NULL ENCODE LZO,
    updated_on TIMESTAMP NULL ENCODE LZO,
    updated_by VARCHAR(100) NULL ENCODE LZO,
    celebrity_order_sync SMALLINT NULL ENCODE LZO,
    retry_counter INTEGER NULL ENCODE DELTA,
    error_message VARCHAR(1000) NULL ENCODE LZO,
    custom_duty DECIMAL(10,3) NULL ENCODE LZO,
    authorization_id VARCHAR(255) NULL ENCODE LZO
);

BEGIN;
CALL public.drop_if_empty_tmp('tmp_inbound_payment_line');
DELETE FROM OFS.inbound_payment_line WHERE 1=1;

INSERT INTO OFS.inbound_payment_line
SELECT id, is_header, web_order_no, item_id, payment_gateway, payment_method_code, cod_charges, shipping_charges, gift_charges, amount, tax, amount_incl_tax, discount, coupon_code, coupon_amount, customized_charges, boutiqaat_credit, other_charges, agent_code, agent_commission, order_charges_processed, mrp_price, unit_price, unit_price_including_tax, wallet_name, wallet_amount, transaction_id, tax_percentage, currency_code, currency_factor, ready_for_archive, inserted_on, inserted_by, updated_on, updated_by, celebrity_order_sync, retry_counter, error_message, custom_duty, authorization_id
FROM tmp_inbound_payment_line;


COMMIT;