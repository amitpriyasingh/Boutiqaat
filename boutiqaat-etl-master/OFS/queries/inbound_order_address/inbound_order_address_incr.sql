BEGIN;
DROP TABLE IF EXISTS tmp_inbound_order_address;
CREATE TEMP TABLE tmp_inbound_order_address(
    id INTEGER,
    web_order_no VARCHAR(50),
    customer_id VARCHAR(30),
    address_detail_type VARCHAR(10),
    first_name VARCHAR(200),
    middle_name VARCHAR(200),
    city VARCHAR(200),
    phone_no VARCHAR(100),
    alternate_phoneno VARCHAR(100),
    post_code VARCHAR(100),
    address_type VARCHAR(20),
    state VARCHAR(200),
    country VARCHAR(45),
    email_id VARCHAR(100),
    ready_for_archive SMALLINT,
    order_date TIMESTAMP,
    inserted_by VARCHAR(100),
    inserted_on TIMESTAMP,
    updated_on TIMESTAMP,
    updated_by VARCHAR(100),
    region VARCHAR(255),
    last_name VARCHAR(100),
    telephone_code VARCHAR(50),
    address_id INTEGER,
    update_address INTEGER    
);

copy tmp_inbound_order_address from '{{S3PATH}}'
iam_role 'arn:aws:iam::652586300051:role/Redshift-athena-S3-readonly'
delimiter '\t'
region 'eu-west-1' 
acceptinvchars
CSV 
GZIP 
emptyasnull
blanksasnull
NULL AS 'null';

INSERT INTO OFS.inbound_order_address
SELECT id, web_order_no, customer_id, address_detail_type, first_name, middle_name, city, phone_no, alternate_phoneno, post_code, address_type, state, country, email_id, ready_for_archive, order_date, inserted_by, inserted_on, updated_on, updated_by, region, last_name, telephone_code, address_id, update_address
FROM tmp_inbound_order_address;


COMMIT;