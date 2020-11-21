BEGIN;
DROP TABLE IF EXISTS tmp_sku_stock;
CREATE TEMP TABLE tmp_sku_stock(
    sku VARCHAR(20),
    sku_name VARCHAR(200),
    category1 VARCHAR(100),
    category2 VARCHAR(100),
    brand VARCHAR(40),
    available INTEGER,
    reserved INTEGER,
    total INTEGER,
    created_at TIMESTAMP,
    updated_at TIMESTAMP,
    synched_at TIMESTAMP
);

copy tmp_sku_stock from '{{S3PATH}}'
iam_role 'arn:aws:iam::652586300051:role/Redshift-athena-S3-readonly'
delimiter '\t'
region 'eu-west-1' 
acceptinvchars
CSV 
GZIP 
emptyasnull
blanksasnull
NULL AS 'null';

CREATE TABLE IF NOT EXISTS aoi.sku_stock (
    sku VARCHAR(20) NULL DISTKEY SORTKEY ENCODE LZO,
    sku_name VARCHAR(200) NULL ENCODE LZO,
    category1 VARCHAR(100) NULL ENCODE LZO,
    category2 VARCHAR(100) NULL ENCODE LZO,
    brand VARCHAR(40) NULL ENCODE LZO,
    available INTEGER NULL ENCODE LZO,
    reserved INTEGER NULL ENCODE LZO,
    total INTEGER NULL ENCODE LZO,
    created_at TIMESTAMP NULL ENCODE LZO,
    updated_at TIMESTAMP NULL ENCODE LZO,
    synched_at TIMESTAMP NULL ENCODE LZO
);

DELETE FROM aoi.sku_stock WHERE 1=1;

INSERT INTO aoi.sku_stock
SELECT sku,sku_name,category1,category2,brand,available,reserved,total,created_at,updated_at,synched_at
FROM tmp_sku_stock;


COMMIT;
