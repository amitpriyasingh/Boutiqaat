BEGIN;
DROP TABLE IF EXISTS tmp_magento_sku_master;
CREATE TEMP TABLE tmp_magento_sku_master(
    sku varchar(64) ,
    sku_name varchar(65535),
    gender varchar(65535),
    config_sku varchar(64),
    size varchar(65535),
    color varchar(65535),
    is_exclusive_to_celebrity SMALLINT,
    is_exclusive_to_boutiqaat varchar(65535),
    special_price decimal(10,2),
    enable_date timestamp
);

copy tmp_magento_sku_master from '{{S3PATH}}'
iam_role 'arn:aws:iam::652586300051:role/Redshift-athena-S3-readonly'
delimiter '\t'
region 'eu-west-1' 
acceptinvchars
CSV 
GZIP 
emptyasnull
blanksasnull
NULL AS 'null';

DROP TABLE IF EXISTS magento.magento_sku_master;
CREATE TABLE IF NOT EXISTS magento.magento_sku_master (
    sku varchar(64),
    sku_name varchar(65535),
    gender varchar(65535),
    config_sku varchar(64),
    size varchar(65535),
    color varchar(65535),
    is_exclusive_to_celebrity SMALLINT,
    is_exclusive_to_boutiqaat varchar(65535),
    special_price decimal(20,6) NULL,
    enable_date timestamp
);

INSERT INTO magento.magento_sku_master
SELECT sku,sku_name,gender,config_sku,size,color,is_exclusive_to_celebrity,is_exclusive_to_boutiqaat,special_price,enable_date
FROM tmp_magento_sku_master;
COMMIT;
