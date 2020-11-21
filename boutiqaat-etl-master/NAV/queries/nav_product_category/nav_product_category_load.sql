BEGIN;
DROP TABLE IF EXISTS tmp_nav_product_category;
CREATE TEMP TABLE tmp_nav_product_category(
sku VARCHAR(20)   ,
sku_name VARCHAR(200)   ,
brand VARCHAR(101)  ,
category1 VARCHAR(40),
supplier_item_no VARCHAR(4000),
total_sellable_qty DECIMAL(10,2),
toal_nav_non_sellable DECIMAL(10,2),
soh decimal(10,2),
stock_refreshed_datetime timestamp
 );

copy tmp_nav_product_category from '{{S3PATH}}'
iam_role 'arn:aws:iam::652586300051:role/Redshift-athena-S3-readonly'
delimiter '\t'
region 'eu-west-1'
acceptinvchars
CSV 
GZIP 
emptyasnull
blanksasnull
NULL AS 'null';


CREATE TABLE IF NOT EXISTS NAV.nav_product_category (
sku VARCHAR(20)  NULL ,
sku_name VARCHAR(200)  NULL ,
brand VARCHAR(101)  NULL,
category1 VARCHAR(40) NULL,
supplier_item_no VARCHAR(4000)  NULL,
total_sellable_qty DECIMAL(10,2),
toal_nav_non_sellable DECIMAL(10,2),
soh decimal(10,2) NULL,
stock_refreshed_datetime timestamp NULL
);

DELETE FROM NAV.nav_product_category WHERE 1=1;

INSERT INTO NAV.nav_product_category
SELECT sku,sku_name,brand,category1,supplier_item_no,total_sellable_qty,toal_nav_non_sellable,soh,stock_refreshed_datetime
FROM tmp_nav_product_category 
group by sku,sku_name,brand,category1,supplier_item_no,total_sellable_qty,toal_nav_non_sellable,soh,stock_refreshed_datetime;
COMMIT;
