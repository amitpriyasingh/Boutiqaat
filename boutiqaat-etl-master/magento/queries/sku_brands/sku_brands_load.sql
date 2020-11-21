BEGIN;
DROP TABLE IF EXISTS tmp_sku_brands;
CREATE TEMP TABLE tmp_sku_brands(
			child_sku varchar(64),
			brand_id integer,
			brand_name  varchar(255)
);

copy tmp_sku_brands from '{{S3PATH}}'
iam_role 'arn:aws:iam::652586300051:role/Redshift-athena-S3-readonly'
delimiter '\t'
region 'eu-west-1' 
acceptinvchars
CSV 
GZIP 
emptyasnull
blanksasnull
NULL AS 'null';

DROP TABLE IF EXISTS magento.sku_brands;
CREATE TABLE IF NOT EXISTS magento.sku_brands (
		sku varchar(64) NULL,
		brand_id integer NULL,
		brand_name  varchar(255) NULL
);

INSERT INTO magento.sku_brands
SELECT *
FROM tmp_sku_brands;
COMMIT;
