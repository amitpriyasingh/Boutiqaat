BEGIN;
DROP TABLE IF EXISTS tmp_customer_demographic_info;
CREATE TEMP TABLE tmp_customer_demographic_info(
    customer_id INTEGER,
    gender VARCHAR(255),
    dob TIMESTAMP,
    email VARCHAR(255),
    phone_number  VARCHAR(255)
);

copy tmp_customer_demographic_info from '{{S3PATH}}'
iam_role 'arn:aws:iam::652586300051:role/Redshift-athena-S3-readonly'
delimiter '\t'
region 'eu-west-1' 
acceptinvchars
CSV 
GZIP 
emptyasnull
blanksasnull
NULL AS 'null';

DROP TABLE IF EXISTS magento.customer_demographic_info;
CREATE TABLE IF NOT EXISTS magento.customer_demographic_info (
    customer_id INTEGER,
    gender VARCHAR(255),
    dob TIMESTAMP,
    email VARCHAR(255),
    phone_number  VARCHAR(255)
);

INSERT INTO magento.customer_demographic_info
SELECT customer_id,gender,dob,email,phone_number FROM tmp_customer_demographic_info;
COMMIT;