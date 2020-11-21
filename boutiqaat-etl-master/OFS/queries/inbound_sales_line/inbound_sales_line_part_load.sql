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
NULL AS 'null'
FILLRECORD
TRUNCATECOLUMNS;

DELETE from OFS.inbound_sales_line WHERE DATE(inserted_on) = DATE('{{DATE}}');
INSERT INTO OFS.inbound_sales_line
SELECT web_order_no, item_id, coupon_code, campaign_id, product_id, description, sku, quantity, bundle_id, bundle_quantity, expected_dispatch_date, item_no, vendor_id, delivery_type, special_delivery_date, is_fragile, is_precious, is_surface, is_customized, pick_location, packaging_location, dsp_code, is_processed, ready_for_archive, inserted_on, inserted_by, updated_on, updated_by, leaf_category, is_foc, is_danger, arabic_description
FROM tmp_inbound_sales_line;
COMMIT;