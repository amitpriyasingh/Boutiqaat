BEGIN;
DROP TABLE IF EXISTS tmp_order_queue;
CREATE TEMP TABLE tmp_order_queue(
    id INTEGER NOT NULL,
    store_id VARCHAR(10),
    device_token VARCHAR(250) NOT NULL,
    device_type VARCHAR(25),
    lang VARCHAR(255) NOT NULL,
    payment VARCHAR(50) NOT NULL,
    myfatoorah_payment VARCHAR(100),
    shipping_address_id INTEGER NOT NULL,
    shipping_method VARCHAR(25) NOT NULL,
    customer_id VARCHAR(250) NOT NULL,
    transaction_id VARCHAR(255),
    tx_amount DECIMAL(10,3) NOT NULL,
    currency VARCHAR(250) NOT NULL,
    reference_order_id INTEGER,
    created_time TIMESTAMP NOT NULL,
    state INTEGER NOT NULL,
    last_updated TIMESTAMP NOT NULL,
    increment_id VARCHAR(20),
    payment_id VARCHAR(100),
    result VARCHAR(100),
    payment_method VARCHAR(100),
    qty_revert INTEGER,
    cod_charge DECIMAL(10,3),
    shipping_charge DECIMAL(10,3),
    shipping_address VARCHAR(max),
    billing_address VARCHAR(max),
    customer_email VARCHAR(250),
    custom_duty DECIMAL(10,3),
    vat DECIMAL(10,3),
    clearance_charge DECIMAL(10,3),
    converted_order_total DECIMAL(10,3),
    notes VARCHAR(max),
    use_customer_balance BOOLEAN,
    base_customer_bal_amount_used DECIMAL(8,4),
    customer_balance_amount_used DECIMAL(8,4),
    credit_revert BOOLEAN,
    coupon_code VARCHAR(255)
);

copy tmp_order_queue from '{{S3PATH}}'
iam_role 'arn:aws:iam::652586300051:role/Redshift-athena-S3-readonly'
delimiter '\t'
region 'eu-west-1' 
acceptinvchars
CSV 
GZIP 
emptyasnull
blanksasnull
NULL AS 'null';

DROP TABLE IF EXISTS magento.order_queue;
CREATE TABLE IF NOT EXISTS magento.order_queue (
    id INTEGER NOT NULL,
    store_id VARCHAR(10),
    device_token VARCHAR(250) NOT NULL,
    device_type VARCHAR(25),
    lang VARCHAR(255) NOT NULL,
    payment VARCHAR(50) NOT NULL,
    myfatoorah_payment VARCHAR(100),
    shipping_address_id INTEGER NOT NULL,
    shipping_method VARCHAR(25) NOT NULL,
    customer_id VARCHAR(250) NOT NULL,
    transaction_id VARCHAR(255),
    tx_amount DECIMAL(10,3) NOT NULL,
    currency VARCHAR(250) NOT NULL,
    reference_order_id INTEGER,
    created_time TIMESTAMP NOT NULL,
    state INTEGER NOT NULL,
    last_updated TIMESTAMP NOT NULL,
    increment_id VARCHAR(20),
    payment_id VARCHAR(100),
    result VARCHAR(100),
    payment_method VARCHAR(100),
    qty_revert INTEGER,
    cod_charge DECIMAL(10,3),
    shipping_charge DECIMAL(10,3),
    shipping_address VARCHAR(max),
    billing_address VARCHAR(max),
    customer_email VARCHAR(250),
    custom_duty DECIMAL(10,3),
    vat DECIMAL(10,3),
    clearance_charge DECIMAL(10,3),
    converted_order_total DECIMAL(10,3),
    notes VARCHAR(max),
    use_customer_balance BOOLEAN,
    base_customer_bal_amount_used DECIMAL(8,4),
    customer_balance_amount_used DECIMAL(8,4),
    credit_revert BOOLEAN,
    coupon_code VARCHAR(255)
);


INSERT INTO magento.order_queue
SELECT id,store_id,device_token,device_type,lang,payment,myfatoorah_payment,shipping_address_id,shipping_method,customer_id,transaction_id,tx_amount,currency,reference_order_id,created_time,state,last_updated,increment_id,payment_id,result,payment_method,qty_revert,cod_charge,shipping_charge,shipping_address,billing_address,customer_email,custom_duty,vat,clearance_charge,converted_order_total,notes,use_customer_balance,base_customer_bal_amount_used,customer_balance_amount_used,credit_revert,coupon_code
FROM tmp_order_queue;
COMMIT;
