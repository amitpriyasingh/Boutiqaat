BEGIN;
DROP TABLE IF EXISTS tmp_notify_out_of_stock;
CREATE TEMP TABLE tmp_notify_out_of_stock(
    id INTEGER,
    entity_id INTEGER,
    email_id VARCHAR(255),
    notified_to_customer VARCHAR(50),
    store INTEGER,
    updated_at timestamp,
    created_at timestamp,
    synced_to_erp VARCHAR(50),
    sms_notified VARCHAR(50),
    category_id VARCHAR(255),
    name VARCHAR(500),
    mobile VARCHAR(100),
    sku VARCHAR(255),
    product_name VARCHAR(500),
    country_code VARCHAR(255)
);

copy tmp_notify_out_of_stock from '{{S3PATH}}'
iam_role 'arn:aws:iam::652586300051:role/Redshift-athena-S3-readonly'
delimiter '\t'
region 'eu-west-1' 
acceptinvchars
CSV 
GZIP 
emptyasnull
blanksasnull
NULL AS 'null';

CREATE TABLE IF NOT EXISTS magento.notify_out_of_stock (
    id INTEGER,
    entity_id INTEGER,
    email_id VARCHAR(255),
    notified_to_customer VARCHAR(50),
    store INTEGER,
    updated_at timestamp,
    created_at timestamp,
    synced_to_erp VARCHAR(50),
    sms_notified VARCHAR(50),
    category_id VARCHAR(255),
    name VARCHAR(500),
    mobile VARCHAR(100),
    sku VARCHAR(255),
    product_name VARCHAR(500),
    country_code VARCHAR(255)
);

DELETE FROM magento.notify_out_of_stock WHERE 1=1;

INSERT INTO magento.notify_out_of_stock
SELECT id,entity_id,email_id,notified_to_customer,store,updated_at,created_at,synced_to_erp,sms_notified,category_id,name,mobile,sku,product_name,country_code
FROM tmp_notify_out_of_stock;
COMMIT;
