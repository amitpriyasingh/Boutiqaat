BEGIN;
DROP TABLE IF EXISTS tmp_sku_categories;
CREATE TEMP TABLE tmp_sku_categories(
			child_sku varchar(64),
			category_id integer,
			category_name  varchar(255)
);

copy tmp_sku_categories from '{{S3PATH}}'
iam_role 'arn:aws:iam::652586300051:role/Redshift-athena-S3-readonly'
delimiter '\t'
region 'eu-west-1' 
acceptinvchars
CSV 
GZIP 
emptyasnull
blanksasnull
NULL AS 'null';

DROP TABLE IF EXISTS magento.sku_categories;
CREATE TABLE IF NOT EXISTS magento.sku_categories (
    		child_sku varchar(64),
    		category_id integer,
			category_name  varchar(255)
);

INSERT INTO magento.sku_categories
SELECT *
FROM tmp_sku_categories;
COMMIT;
