DROP TABLE IF EXISTS tmp_celebrity_product;
CREATE TEMP TABLE tmp_celebrity_product(
    product_id INTEGER,
    celebrity_id INTEGER,
    ad_number INTEGER,
    ad_date TIMESTAMP,
    product_entity_id INTEGER,
    website_id SMALLINT,
    is_exclusive VARCHAR(50),
    boutique_price DECIMAL(12,4),
    commission DECIMAL(12,4),
    max_order INTEGER,
    reorder_qty INTEGER,
    celebrity_sale INTEGER,
    grn_number INTEGER,
    product_status VARCHAR(50),
    product_mapping_status SMALLINT,
    updated_by VARCHAR(50),
    first_online_date TIMESTAMP,
    sort_order INTEGER,
    created_at TIMESTAMP,
    updated_at TIMESTAMP,
    type_id TEXT,
    child_product VARCHAR(max),
    product_state VARCHAR(50),
    is_edited VARCHAR(100),
    available_balance SMALLINT
);

copy tmp_celebrity_product from '{{S3PATH}}'
iam_role 'arn:aws:iam::652586300051:role/Redshift-athena-S3-readonly'
delimiter '\t'
region 'eu-west-1' 
acceptinvchars
CSV 
GZIP 
emptyasnull
blanksasnull
NULL AS 'null';

CREATE TABLE IF NOT EXISTS magento.celebrity_product (
    product_id INTEGER NOT NULL ENCODE ZSTD PRIMARY KEY,
    celebrity_id INTEGER NOT NULL ENCODE ZSTD DISTKEY SORTKEY,
    ad_number INTEGER NOT NULL ENCODE ZSTD,
    ad_date TIMESTAMP NOT NULL ENCODE ZSTD,
    product_entity_id INTEGER NOT NULL ENCODE ZSTD,
    website_id SMALLINT NOT NULL ENCODE ZSTD,
    is_exclusive VARCHAR(50) NOT NULL ENCODE ZSTD,
    boutique_price DECIMAL(12,4) NOT NULL ENCODE ZSTD,
    commission DECIMAL(12,4) NULL ENCODE ZSTD,
    max_order INTEGER NULL ENCODE ZSTD,
    reorder_qty INTEGER NULL ENCODE ZSTD,
    celebrity_sale INTEGER NULL ENCODE ZSTD,
    grn_number INTEGER NULL ENCODE ZSTD,
    product_status VARCHAR(50) NULL ENCODE ZSTD,
    product_mapping_status SMALLINT NOT NULL ENCODE ZSTD,
    updated_by VARCHAR(50) NULL ENCODE ZSTD,
    first_online_date TIMESTAMP NULL ENCODE ZSTD,
    sort_order INTEGER NOT NULL ENCODE ZSTD,
    created_at TIMESTAMP NULL ENCODE ZSTD,
    updated_at TIMESTAMP NULL ENCODE ZSTD,
    type_id TEXT NULL ENCODE ZSTD,
    child_product VARCHAR(max) NULL ENCODE ZSTD,
    product_state VARCHAR(50) NULL ENCODE ZSTD,
    is_edited VARCHAR(100) NOT NULL ENCODE ZSTD,
    available_balance SMALLINT NOT NULL ENCODE ZSTD
);

BEGIN;
CALL public.drop_if_empty_tmp('tmp_celebrity_product');
DELETE FROM magento.celebrity_product WHERE 1=1;

INSERT INTO magento.celebrity_product
SELECT product_id,celebrity_id,ad_number,ad_date,product_entity_id,website_id,is_exclusive,boutique_price,commission,max_order,reorder_qty,celebrity_sale,grn_number,product_status,product_mapping_status,updated_by,first_online_date,sort_order,created_at,updated_at,type_id,child_product,product_state,is_edited,available_balance
FROM tmp_celebrity_product;
COMMIT;

