BEGIN;
DROP TABLE IF EXISTS tmp_inbound_sales_line;
CREATE TEMP TABLE tmp_inbound_sales_line(
    web_order_no VARCHAR(50),
    item_id INTEGER,
    coupon_code VARCHAR(100),
    campaign_id VARCHAR(100),
    product_id VARCHAR(45),
    description VARCHAR(500),
    sku VARCHAR(255),
    quantity SMALLINT,
    bundle_id VARCHAR(100),
    bundle_quantity INTEGER,
    expected_dispatch_date TIMESTAMP,
    item_no VARCHAR(20),
    vendor_id INTEGER,
    delivery_type VARCHAR(20),
    special_delivery_date TIMESTAMP,
    is_fragile SMALLINT,
    is_precious SMALLINT,
    is_surface SMALLINT,
    is_customized SMALLINT,
    pick_location VARCHAR(50),
    packaging_location VARCHAR(100),
    dsp_code VARCHAR(45),
    is_processed SMALLINT,
    ready_for_archive SMALLINT,
    inserted_on TIMESTAMP,
    inserted_by VARCHAR(100),
    updated_on TIMESTAMP,
    updated_by VARCHAR(100),
    leaf_category VARCHAR(255),
    is_foc BOOLEAN,
    is_danger BOOLEAN,
    arabic_description VARCHAR(500)
);

copy tmp_inbound_sales_line from '{{S3PATH}}'
iam_role 'arn:aws:iam::652586300051:role/Redshift-athena-S3-readonly'
delimiter '\t'
region 'eu-west-1' 
acceptinvchars
CSV 
GZIP 
emptyasnull
blanksasnull
NULL AS 'null';


CREATE TABLE IF NOT EXISTS OFS.inbound_sales_line (
    web_order_no VARCHAR(50) NULL ENCODE LZO,
    item_id INTEGER NOT NULL ENCODE LZO SORTKEY DISTKEY PRIMARY KEY,
    coupon_code VARCHAR(100) NULL ENCODE LZO,
    campaign_id VARCHAR(100) NULL ENCODE LZO,
    product_id VARCHAR(45) NULL ENCODE LZO,
    description VARCHAR(500) NULL ENCODE LZO,
    sku VARCHAR(255) NULL ENCODE LZO,
    quantity SMALLINT NULL ENCODE LZO,
    bundle_id VARCHAR(100) NULL ENCODE LZO,
    bundle_quantity INTEGER NULL ENCODE DELTA,
    expected_dispatch_date TIMESTAMP NULL ENCODE LZO,
    item_no VARCHAR(20) NULL ENCODE LZO,
    vendor_id INTEGER NULL ENCODE DELTA,
    delivery_type VARCHAR(20) NULL ENCODE LZO,
    special_delivery_date TIMESTAMP NULL ENCODE LZO,
    is_fragile SMALLINT NULL ENCODE DELTA,
    is_precious SMALLINT NULL ENCODE DELTA,
    is_surface SMALLINT NULL ENCODE DELTA,
    is_customized SMALLINT NULL ENCODE DELTA,
    pick_location VARCHAR(50) NULL ENCODE LZO,
    packaging_location VARCHAR(100) NULL ENCODE LZO,
    dsp_code VARCHAR(45) NULL ENCODE LZO,
    is_processed SMALLINT NOT NULL ENCODE DELTA,
    ready_for_archive SMALLINT NOT NULL ENCODE DELTA,
    inserted_on TIMESTAMP NOT NULL ENCODE LZO,
    inserted_by VARCHAR(100) NULL ENCODE LZO,
    updated_on TIMESTAMP NULL ENCODE LZO,
    updated_by VARCHAR(100) NULL ENCODE LZO,
    leaf_category VARCHAR(255) NULL ENCODE LZO,
    is_foc BOOLEAN NOT NULL ENCODE ZSTD,
    is_danger BOOLEAN NULL ENCODE ZSTD,
    arabic_description VARCHAR(500) NULL ENCODE LZO    
);

DELETE FROM OFS.inbound_sales_line where 1=1;

INSERT INTO OFS.inbound_sales_line
SELECT web_order_no, item_id, coupon_code, campaign_id, product_id, description, sku, quantity, bundle_id, bundle_quantity, expected_dispatch_date, item_no, vendor_id, delivery_type, special_delivery_date, is_fragile, is_precious, is_surface, is_customized, pick_location, packaging_location, dsp_code, is_processed, ready_for_archive, inserted_on, inserted_by, updated_on, updated_by, leaf_category, is_foc, is_danger, arabic_description
FROM tmp_inbound_sales_line;


COMMIT;
