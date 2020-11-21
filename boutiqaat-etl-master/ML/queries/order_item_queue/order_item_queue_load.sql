BEGIN;
DROP TABLE IF EXISTS tmp_order_item_queue;
CREATE TEMP TABLE tmp_order_item_queue(
    id INTEGER,
    order_temp_id INTEGER,
    quantity INTEGER,
    product_id INTEGER,
    parent_product_id INTEGER,
    unit_price DECIMAL(10,3),
    total_price DECIMAL(10,3),
    name VARCHAR(1000),
    sku VARCHAR(250),
    celebrity_id INTEGER,
    created_time TIMESTAMP,
    last_updated TIMESTAMP,
    tv_id INTEGER,
    image_url VARCHAR(250),
    discounted_price DECIMAL(8,4),
    is_foc SMALLINT NOT NULL
);

copy tmp_order_item_queue from '{{S3PATH}}'
iam_role 'arn:aws:iam::652586300051:role/Redshift-athena-S3-readonly'
delimiter '\t'
region 'eu-west-1' 
acceptinvchars
CSV 
GZIP 
emptyasnull
blanksasnull
NULL AS 'null';

DROP TABLE IF EXISTS magento.order_item_queue;
CREATE TABLE IF NOT EXISTS magento.order_item_queue (
    id INTEGER NOT NULL,
    order_temp_id INTEGER NOT NULL,
    quantity INTEGER NOT NULL,
    product_id INTEGER NOT NULL,
    parent_product_id INTEGER,
    unit_price DECIMAL(10,3),
    total_price DECIMAL(10,3),
    name VARCHAR(1000),
    sku VARCHAR(250) NOT NULL,
    celebrity_id INTEGER NOT NULL,
    created_time TIMESTAMP NOT NULL,
    last_updated TIMESTAMP,
    tv_id INTEGER,
    image_url VARCHAR(250),
    discounted_price DECIMAL(8,4),
    is_foc SMALLINT NOT NULL
);

INSERT INTO magento.order_item_queue
SELECT id,order_temp_id,quantity,product_id,parent_product_id,unit_price,total_price,name,sku,celebrity_id,created_time,last_updated,tv_id,image_url,discounted_price,is_foc
FROM tmp_order_item_queue;
COMMIT;