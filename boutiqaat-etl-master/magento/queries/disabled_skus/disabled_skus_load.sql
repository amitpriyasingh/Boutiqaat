BEGIN;
DROP TABLE IF EXISTS tmp_disabled_skus;
CREATE TEMP TABLE tmp_disabled_skus(
    id INTEGER,
    sku VARCHAR(100),
    product_id INTEGER,
    status SMALLINT,
    created_at TIMESTAMP,
    updated_at TIMESTAMP
);

copy tmp_disabled_skus from '{{S3PATH}}'
iam_role 'arn:aws:iam::652586300051:role/Redshift-athena-S3-readonly'
delimiter '\t'
region 'eu-west-1' 
acceptinvchars
CSV 
GZIP 
emptyasnull
blanksasnull
NULL AS 'null';

DROP TABLE IF EXISTS magento.disabled_skus;
CREATE TABLE magento.disabled_skus (
    id INTEGER NOT NULL ENCODE LZO  PRIMARY KEY,
    sku VARCHAR(100) NULL ENCODE LZO DISTKEY,
    product_id INTEGER NULL ENCODE LZO,
    status SMALLINT NULL ENCODE DELTA,
    created_at TIMESTAMP NOT NULL ENCODE LZO SORTKEY,
    updated_at TIMESTAMP NOT NULL ENCODE LZO
);

INSERT INTO magento.disabled_skus
SELECT id, sku, product_id, status, created_at, updated_at
FROM tmp_disabled_skus;
COMMIT;

