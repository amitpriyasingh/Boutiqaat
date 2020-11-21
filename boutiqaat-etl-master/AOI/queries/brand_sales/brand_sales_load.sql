BEGIN;
DROP TABLE IF EXISTS tmp_brand_sales;
CREATE TEMP TABLE tmp_brand_sales(
order_date TIMESTAMP NULL,
  brand varchar(80)  NULL,
  total_sales DECIMAL(47,2) NULL,
  total_quantity decimal(47,0) NULL,
  celebrity_qty decimal(47,0) NULL,
  celebrity_revenue DECIMAL(47,2) NULL,
  cancelled_qty decimal(47,0) NULL,
  cancelled_revenue DECIMAL(47,2) NULL,
  stock_qty decimal(32,0) NULL,
  reserved_qty decimal(32,0) NULL,
  department varchar(18)  NULL
);

copy tmp_brand_sales from '{{S3PATH}}'
iam_role 'arn:aws:iam::652586300051:role/Redshift-athena-S3-readonly'
delimiter '\t'
region 'eu-west-1' 
acceptinvchars
CSV 
GZIP 
emptyasnull
blanksasnull
NULL AS 'null';

CREATE TABLE IF NOT EXISTS aoi.brand_sales (
    order_date TIMESTAMP NULL ENCODE ZSTD,
  brand varchar(80)  NULL ENCODE ZSTD,
  total_sales DECIMAL(47,2) NULL ENCODE ZSTD,
  total_quantity decimal(47,0) NULL ENCODE ZSTD,
  celebrity_qty decimal(47,0) NULL ENCODE ZSTD,
  celebrity_revenue DECIMAL(47,2) NULL ENCODE ZSTD,
  cancelled_qty decimal(47,0) NULL ENCODE ZSTD,
  cancelled_revenue DECIMAL(47,2) NULL ENCODE ZSTD,
  stock_qty decimal(32,0) NULL ENCODE ZSTD,
  reserved_qty decimal(32,0) NULL ENCODE ZSTD,
  department varchar(18)  NULL ENCODE ZSTD,
);

DELETE FROM aoi.brand_sales WHERE 1=1;

INSERT INTO aoi.brand_sales
SELECT order_date, brand, total_sales, total_quantity, celebrity_qty, celebrity_revenue, cancelled_qty, cancelled_revenue, stock_qty, reserved_qty, department
FROM tmp_brand_sales;


COMMIT;
