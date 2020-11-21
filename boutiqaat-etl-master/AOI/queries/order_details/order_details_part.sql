BEGIN;
DROP TABLE IF EXISTS tmp_order_details;
CREATE TEMP TABLE tmp_order_details(
    order_number VARCHAR(50),
    app_order_number varchar(50),
    item_id INTEGER,
    bundle_id VARCHAR(100),
    bundle_seq_id VARCHAR(100), 
    batch_id BIGINT,
    awbno VARCHAR(20),
    dsp_code VARCHAR(50),
    sku VARCHAR(30),
    sku_name TEXT,
    category1 VARCHAR(80),
    category2 VARCHAR(80),
    brand VARCHAR(80),
    gender VARCHAR(10),
    celebrity_code VARCHAR(10),
    celebrity_id INTEGER,
    celebrity_name VARCHAR(100),
    account_manager VARCHAR(100),
    quantity SMALLINT,
    order_currency VARCHAR(20),
    exchange_rate DECIMAL(10,3),
    net_sale_price DECIMAL(10,3),
    rrp DECIMAL(10,3),
    list_price DECIMAL(11,3),
    shipping_charge DECIMAL(10,3),
    cod_charge DECIMAL(10,3),
    allocated_order_count DECIMAL(10,3),
    order_date DATE,
    order_at TIMESTAMP,
    order_date_utc DATE,
    order_at_utc TIMESTAMP,
    order_type VARCHAR(15),
    order_category VARCHAR(50),
    payment_method VARCHAR(30),
    payment_gateway VARCHAR(60),
    customer_id VARCHAR(30),
    billing_phone_no VARCHAR(100),
    billing_country VARCHAR(45),
    shipping_phone_no VARCHAR(100),
    shipping_country VARCHAR(45),
    net_sale_price_kwd DECIMAL(10,3),
    shipping_charge_kwd DECIMAL(10,3),
    cod_charge_kwd DECIMAL(10,3),
    order_status_id INTEGER,
    order_status VARCHAR(50),
    status_grouped_mgmt VARCHAR(80),
    status_grouped_ops VARCHAR(80),
    last_activity VARCHAR(50),
    status_at TIMESTAMP,
    cancelled_at TIMESTAMP,
    confirmed_at TIMESTAMP,
    readytoship_at TIMESTAMP,
    picked_at TIMESTAMP,
    manifested_at TIMESTAMP,
    order_allocated_at TIMESTAMP,
    packed_at TIMESTAMP,
    shipped_at TIMESTAMP,
    delivered_at TIMESTAMP,
    returned_at TIMESTAMP,
    batch_inserted_at TIMESTAMP,
    order_inserted_on_utc TIMESTAMP,
    is_ndr SMALLINT,
    updated_at_utc TIMESTAMP
);

copy tmp_order_details from '{{S3PATH}}'
iam_role 'arn:aws:iam::652586300051:role/Redshift-athena-S3-readonly'
delimiter '\t'
region 'eu-west-1' 
acceptinvchars
CSV 
GZIP 
emptyasnull
blanksasnull
NULL AS 'null';

DELETE FROM aoi.order_details WHERE order_at >= '{{PDATE}}';
INSERT INTO aoi.order_details
SELECT order_number,app_order_number,item_id,bundle_id,bundle_seq_id,batch_id,awbno,dsp_code,sku,sku_name,category1,category2,brand,gender,celebrity_code,celebrity_id,celebrity_name,account_manager,quantity,order_currency,exchange_rate,net_sale_price,rrp,list_price,shipping_charge,cod_charge,allocated_order_count,order_date,order_at,order_date_utc,order_at_utc,order_type,order_category,payment_method,payment_gateway,customer_id,billing_phone_no,billing_country,shipping_phone_no,shipping_country,net_sale_price_kwd,shipping_charge_kwd,cod_charge_kwd,order_status_id,order_status,status_grouped_mgmt,status_grouped_ops,last_activity,status_at,cancelled_at,confirmed_at,readytoship_at,picked_at,manifested_at,order_allocated_at,packed_at,shipped_at,delivered_at,returned_at,batch_inserted_at,order_inserted_on_utc,is_ndr,updated_at_utc
FROM tmp_order_details;


COMMIT;

