DROP TABLE IF EXISTS tmp_catalog_product_entity;
CREATE TEMP TABLE tmp_catalog_product_entity(
    row_id INTEGER,
    entity_id INTEGER,
    created_in BIGINT,
    updated_in BIGINT,
    attribute_set_id SMALLINT,
    type_id VARCHAR(32),
    sku VARCHAR(64),
    has_options SMALLINT,
    required_options SMALLINT,
    created_at TIMESTAMP,
    updated_at TIMESTAMP
);

copy tmp_catalog_product_entity from '{{S3PATH}}'
iam_role 'arn:aws:iam::652586300051:role/Redshift-athena-S3-readonly'
delimiter '\t'
region 'eu-west-1' 
acceptinvchars
CSV 
GZIP 
emptyasnull
blanksasnull
NULL AS 'null';

CREATE TABLE IF NOT EXISTS magento.catalog_product_entity (
    row_id INTEGER NOT NULL ENCODE LZO PRIMARY KEY,
    entity_id INTEGER NOT NULL ENCODE LZO,
    created_in BIGINT NOT NULL ENCODE LZO,
    updated_in BIGINT NOT NULL ENCODE LZO,
    attribute_set_id SMALLINT NOT NULL ENCODE LZO,
    type_id VARCHAR(32) NOT NULL ENCODE LZO,
    sku VARCHAR(64) NULL ENCODE LZO DISTKEY,
    has_options SMALLINT NOT NULL ENCODE DELTA,
    required_options SMALLINT NOT NULL ENCODE DELTA,
    created_at TIMESTAMP NOT NULL ENCODE LZO SORTKEY,
    updated_at TIMESTAMP NOT NULL ENCODE LZO
);

BEGIN;
CALL public.drop_if_empty_tmp('tmp_catalog_product_entity');
DELETE FROM magento.catalog_product_entity WHERE 1=1;

INSERT INTO magento.catalog_product_entity
SELECT row_id, entity_id, created_in, updated_in, attribute_set_id, type_id, sku, has_options, required_options, created_at, updated_at
FROM tmp_catalog_product_entity;
COMMIT;
