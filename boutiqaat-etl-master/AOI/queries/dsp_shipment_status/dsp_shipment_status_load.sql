BEGIN;
DROP TABLE IF EXISTS tmp_dsp_shipment_status;
CREATE TEMP TABLE tmp_dsp_shipment_status(
    waybill_no varchar(255), 
    update_time TIMESTAMP, 
    update_location varchar(255), 
    city_code varchar(3), 
    update_code VARCHAR(500), 
    update_description VARCHAR(500),
    awbno varchar(20), 
    order_no varchar(50), 
    order_currency varchar(20), 
    order_date TIMESTAMP, 
    order_at TIMESTAMP, 
    status_grouped_mgmt varchar(80),
    shipping_phone varchar(100), 
    billing_phone varchar(100), 
    email varchar(100), 
    city varchar(50), 
    shipped_at TIMESTAMP, 
    net_sale DECIMAL(20,3), 
    sync_at_utc TIMESTAMP
);

copy tmp_dsp_shipment_status from '{{S3PATH}}'
iam_role 'arn:aws:iam::652586300051:role/Redshift-athena-S3-readonly'
delimiter '\t'
region 'eu-west-1' 
acceptinvchars
CSV 
GZIP 
emptyasnull
blanksasnull
NULL AS 'null';


DROP TABLE IF EXISTS aoi.dsp_shipment_status;
CREATE TABLE IF NOT EXISTS aoi.dsp_shipment_status (
    waybill_no varchar(255), 
    update_time TIMESTAMP, 
    update_location varchar(255), 
    city_code varchar(3), 
    update_code VARCHAR(500), 
    update_description VARCHAR(500),
    awbno varchar(20), 
    order_no varchar(50), 
    order_currency varchar(20), 
    order_date TIMESTAMP, 
    order_at TIMESTAMP, 
    status_grouped_mgmt varchar(80),
    shipping_phone varchar(100), 
    billing_phone varchar(100), 
    email varchar(100), 
    city varchar(50), 
    shipped_at TIMESTAMP, 
    net_sale DECIMAL(20,3), 
    sync_at_utc TIMESTAMP
);


INSERT INTO aoi.dsp_shipment_status
SELECT waybill_no,update_time,update_location,city_code,update_code,update_description,awbno,order_no,order_currency,order_date,order_at,status_grouped_mgmt,shipping_phone,billing_phone,email,city,shipped_at,net_sale,sync_at_utc
FROM tmp_dsp_shipment_status;

COMMIT;
