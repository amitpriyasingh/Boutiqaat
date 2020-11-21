BEGIN;
DROP TABLE IF EXISTS tmp_magento_customerbalance_history;
CREATE TEMP TABLE tmp_magento_customerbalance_history(
    history_id INTEGER,
    balance_id INTEGER,
    updated_at TIMESTAMP,
    action SMALLINT,
    balance_amount DECIMAL(20,4),
    balance_delta DECIMAL(20,4),
    additional_info VARCHAR(255),
    is_customer_notified SMALLINT,
    ticket VARCHAR(255)
);

copy tmp_magento_customerbalance_history from '{{S3PATH}}'
iam_role 'arn:aws:iam::652586300051:role/Redshift-athena-S3-readonly'
delimiter '\t'
region 'eu-west-1' 
acceptinvchars
CSV 
GZIP 
emptyasnull
blanksasnull
NULL AS 'null';

DROP TABLE IF EXISTS magento.magento_customerbalance_history;
CREATE TABLE magento.magento_customerbalance_history (
    history_id INTEGER NOT NULL ENCODE ZSTD DISTKEY SORTKEY PRIMARY KEY,
    balance_id INTEGER NOT NULL ENCODE ZSTD,
    updated_at TIMESTAMP NULL ENCODE ZSTD,
    action SMALLINT NOT NULL ENCODE ZSTD,
    balance_amount DECIMAL(20,4) NOT NULL ENCODE ZSTD,
    balance_delta DECIMAL(20,4) NOT NULL ENCODE ZSTD,
    additional_info VARCHAR(255) NULL ENCODE ZSTD,
    is_customer_notified SMALLINT NOT NULL ENCODE ZSTD,
    ticket VARCHAR(255) NULL ENCODE ZSTD
);

INSERT INTO magento.magento_customerbalance_history
SELECT history_id, balance_id, updated_at, action, balance_amount, balance_delta, additional_info, is_customer_notified, ticket
FROM tmp_magento_customerbalance_history;
COMMIT;

