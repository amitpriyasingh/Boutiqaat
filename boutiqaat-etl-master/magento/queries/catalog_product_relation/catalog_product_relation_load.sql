DROP TABLE IF EXISTS tmp_catalog_product_relation;
CREATE TEMP TABLE tmp_catalog_product_relation(
  parent_id INTEGER,
  child_id INTEGER
);

copy tmp_catalog_product_relation from '{{S3PATH}}'
iam_role 'arn:aws:iam::652586300051:role/Redshift-athena-S3-readonly'
delimiter '\t'
region 'eu-west-1' 
acceptinvchars
CSV 
GZIP 
emptyasnull
blanksasnull
NULL AS 'null';

CREATE TABLE IF NOT EXISTS magento.catalog_product_relation (
    parent_id INTEGER NOT NULL ENCODE LZO DISTKEY SORTKEY,
    child_id INTEGER NOT NULL ENCODE LZO
);

BEGIN;
CALL public.drop_if_empty_tmp('tmp_catalog_product_relation');
DELETE FROM magento.catalog_product_relation WHERE 1=1;

INSERT INTO magento.catalog_product_relation
SELECT parent_id,child_id
FROM tmp_catalog_product_relation;
COMMIT;
