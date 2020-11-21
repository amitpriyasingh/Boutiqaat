BEGIN;
DROP TABLE IF EXISTS tmp_magento_customerbalance;
CREATE TEMP TABLE tmp_magento_customerbalance(
    balance_id INTEGER,
    customer_id INTEGER,
    website_id SMALLINT,
    amount DECIMAL(20,4),
    base_currency_code VARCHAR(3)
);

copy tmp_magento_customerbalance from '{{S3PATH}}'
iam_role 'arn:aws:iam::652586300051:role/Redshift-athena-S3-readonly'
delimiter '\t'
region 'eu-west-1' 
acceptinvchars
CSV 
GZIP 
emptyasnull
blanksasnull
NULL AS 'null';

DROP TABLE IF EXISTS magento.magento_customerbalance;
CREATE TABLE magento.magento_customerbalance (
    balance_id INTEGER NOT NULL ENCODE ZSTD DISTKEY SORTKEY PRIMARY KEY,
    customer_id INTEGER NOT NULL ENCODE ZSTD,
    website_id SMALLINT NULL ENCODE DELTA,
    amount DECIMAL(20,4) NOT NULL ENCODE ZSTD,
    base_currency_code VARCHAR(3) NULL ENCODE ZSTD
);

INSERT INTO magento.magento_customerbalance
SELECT balance_id,customer_id,website_id,amount,base_currency_code
FROM tmp_magento_customerbalance;
COMMIT;

