BEGIN;
DROP TABLE IF EXISTS tmp_sku_master;
CREATE TEMP TABLE tmp_sku_master(
    sku VARCHAR(64),
    parent_sku VARCHAR(64),
    sku_type VARCHAR(12),
    created_date TIMESTAMP,
    sku_name VARCHAR(2000),
    sku_gender VARCHAR(50),
    brand_id VARCHAR(50),
    brand VARCHAR(255),
    boutiqaat_exclusive VARCHAR(2000),
    celebrity_exclusive SMALLINT,
    category1_id VARCHAR(500),
    category2_id VARCHAR(500),
    category3_id VARCHAR(500),
    category4_id VARCHAR(500),
    category1 VARCHAR(500),
    category2 VARCHAR(500),
    category3 VARCHAR(500),
    category4 VARCHAR(500),
    first_online_date TIMESTAMP,
    is_disabled_now SMALLINT,
    current_enable_status SMALLINT,
    size VARCHAR(50),
    color VARCHAR(50),
    price DECIMAL(20,6),
    special_price DECIMAL(20,6),
    image_url VARCHAR(1000),
    category1_list VARCHAR(500),
    category2_list VARCHAR(500),
    category3_list VARCHAR(500),
    category4_list VARCHAR(500),
    category1_list_id VARCHAR(500),
    category2_list_id VARCHAR(500),
    category3_list_id VARCHAR(500),
    category4_list_id VARCHAR(500),
    child_id INTEGER,
    parent_id INTEGER
);

copy tmp_sku_master from '{{S3PATH}}'
iam_role 'arn:aws:iam::652586300051:role/Redshift-athena-S3-readonly'
delimiter '\t'
region 'eu-west-1' 
acceptinvchars
CSV 
GZIP 
emptyasnull
blanksasnull
NULL AS 'null';

DROP TABLE IF EXISTS magento.sku_master;
CREATE TABLE IF NOT EXISTS magento.sku_master (
    sku VARCHAR(64),
    parent_sku VARCHAR(64),
    sku_type VARCHAR(12),
    created_date TIMESTAMP,
    sku_name VARCHAR(2000),
    sku_gender VARCHAR(50),
    brand_id VARCHAR(50),
    brand VARCHAR(255),
    boutiqaat_exclusive VARCHAR(2000),
    celebrity_exclusive SMALLINT,
    category1_id VARCHAR(500),
    category2_id VARCHAR(500),
    category3_id VARCHAR(500),
    category4_id VARCHAR(500),
    category1 VARCHAR(500),
    category2 VARCHAR(500),
    category3 VARCHAR(500),
    category4 VARCHAR(500),
    first_online_date TIMESTAMP,
    is_disabled_now SMALLINT,
    current_enable_status SMALLINT,
    size VARCHAR(50),
    color VARCHAR(50),
    price DECIMAL(20,6),
    special_price DECIMAL(20,6),
    image_url VARCHAR(1000),
    category1_list VARCHAR(500),
    category2_list VARCHAR(500),
    category3_list VARCHAR(500),
    category4_list VARCHAR(500),
    category1_list_id VARCHAR(500),
    category2_list_id VARCHAR(500),
    category3_list_id VARCHAR(500),
    category4_list_id VARCHAR(500),
    child_id INTEGER,
    parent_id INTEGER
);

INSERT INTO magento.sku_master
SELECT sku, parent_sku, sku_type, created_date, sku_name, sku_gender, brand_id, brand, boutiqaat_exclusive, celebrity_exclusive, category1_id, category2_id, category3_id, category4_id, category1, category2, category3, category4, first_online_date, is_disabled_now, current_enable_status, size, color, price, special_price, image_url, category1_list, category2_list, category3_list, category4_list, category1_list_id, category2_list_id, category3_list_id, category4_list_id, child_id, parent_id
FROM tmp_sku_master;
COMMIT;

