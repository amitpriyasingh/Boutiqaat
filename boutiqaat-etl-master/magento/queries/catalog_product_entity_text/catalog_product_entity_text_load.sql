DROP TABLE IF EXISTS tmp_catalog_product_entity_text;
CREATE TEMP TABLE tmp_catalog_product_entity_text(
    value_id INTEGER,
    attribute_id SMALLINT,
    store_id SMALLINT,
    row_id INTEGER,
    value VARCHAR(max)
);

copy tmp_catalog_product_entity_text from '{{S3PATH}}'
iam_role 'arn:aws:iam::652586300051:role/Redshift-athena-S3-readonly'
delimiter '\t'
region 'eu-west-1' 
acceptinvchars
CSV 
GZIP 
emptyasnull
blanksasnull
NULL AS 'null';

CREATE TABLE  IF NOT EXISTS magento.catalog_product_entity_text (
    value_id INTEGER NOT NULL ENCODE LZO PRIMARY KEY SORTKEY,
    attribute_id SMALLINT NOT NULL ENCODE DELTA,
    store_id SMALLINT NOT NULL ENCODE DELTA DISTKEY,
    row_id INTEGER NOT NULL ENCODE LZO,
    value VARCHAR(max) NULL ENCODE LZO
);

BEGIN;
CALL public.drop_if_empty_tmp('tmp_catalog_product_entity_text');
DELETE FROM magento.catalog_product_entity_text WHERE 1=1;

INSERT INTO magento.catalog_product_entity_text
SELECT value_id, attribute_id, store_id, row_id, value
FROM tmp_catalog_product_entity_text;
COMMIT;


