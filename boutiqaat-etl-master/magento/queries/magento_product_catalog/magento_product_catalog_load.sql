BEGIN;
DROP TABLE IF EXISTS tmp_magento_product_catalog;
CREATE TEMP TABLE tmp_magento_product_catalog(
    row_id integer,
    sku varchar(64) ,
    parent_sku varchar(64),
    color varchar(255),
	brand_id integer,
	brand_name  varchar(255),
    price decimal(20,6),
	category_id integer,
    category1 varchar(255),
    category2 varchar(255),
    category3 varchar(255),
    category4 varchar(255),
    category5 varchar(255),
    image varchar(255)
);

copy tmp_magento_product_catalog from '{{S3PATH}}'
iam_role 'arn:aws:iam::652586300051:role/Redshift-athena-S3-readonly'
delimiter '\t'
region 'eu-west-1' 
acceptinvchars
CSV 
GZIP 
emptyasnull
blanksasnull
NULL AS 'null';

CREATE TABLE IF NOT EXISTS magento.magento_product_catalog (
    row_id integer NULL,
    sku varchar(64) NULL,
    parent_sku varchar(64) NULL,
    color varchar(255) NULL,
	brand_id integer NULL,
	brand_name  varchar(255) NULL,    
    price decimal(20,6) NULL,
	category_id integer NULL,
    category1 varchar(255) NULL,
    category2 varchar(255) NULL,
    category3 varchar(255) NULL,
    category4 varchar(255) NULL,
    category5 varchar(255) NULL,
    image varchar(255) NULL
);

DELETE FROM magento.magento_product_catalog WHERE 1=1;

INSERT INTO magento.magento_product_catalog
SELECT row_id,sku,parent_sku,color,brand_id,brand_name,price,category_id,category1,category2,category3,category4,category5,image
FROM tmp_magento_product_catalog;
COMMIT;
