BEGIN;
DROP TABLE IF EXISTS tmp_brand_target_sales_master_details;
CREATE TEMP TABLE tmp_brand_target_sales_master_details(
    traget_month INTEGER, 
    brand_target_owner VARCHAR(100), 
    brand_target_owner_email VARCHAR(100), 
    brand_target_owner_type VARCHAR(100), 
    brand VARCHAR(80), 
    celebrity_name VARCHAR(100), 
    mtd_target DECIMAL(10,3), 
    total_target DECIMAL(10,3),
    mtd_sale DECIMAL(10,3), 
    monthly_projected_sale DECIMAL(10,3), 
    mtd_achievement DECIMAL(10,3),
    monthly_projected_achievement DECIMAL(10,3)
    
);

copy tmp_brand_target_sales_master_details from '{{S3PATH}}'
iam_role 'arn:aws:iam::652586300051:role/Redshift-athena-S3-readonly'
delimiter '\t'
region 'eu-west-1' 
acceptinvchars
CSV 
GZIP 
emptyasnull
blanksasnull
NULL AS 'null';


CREATE TABLE IF NOT EXISTS aoi.brand_target_sales_master_details (
    traget_month INTEGER NOT NULL ENCODE LZO, 
    brand_target_owner VARCHAR(100) NULL ENCODE LZO,
    brand_target_owner_email VARCHAR(100) NULL ENCODE LZO, 
    brand_target_owner_type VARCHAR(100) NULL ENCODE LZO, 
    brand VARCHAR(80) NULL ENCODE LZO,
    celebrity_name VARCHAR(100) NULL ENCODE LZO, 
    mtd_target DECIMAL(10,3)NOT NULL ENCODE LZO, 
    total_target DECIMAL(10,3)NOT NULL ENCODE LZO,
    mtd_sale DECIMAL(10,3) NOT NULL ENCODE LZO, 
    monthly_projected_sale DECIMAL(10,3) NOT NULL ENCODE LZO, 
    mtd_achievement DECIMAL(10,3) NOT NULL ENCODE LZO,
    monthly_projected_achievement DECIMAL(10,3) NOT NULL ENCODE LZO

);

DELETE FROM aoi.brand_target_sales_master_details WHERE 1=1;

INSERT INTO aoi.brand_target_sales_master_details
SELECT traget_month, brand_target_owner, brand_target_owner_email, brand_target_owner_type, brand, celebrity_name, mtd_target, total_target, mtd_sale, monthly_projected_sale, mtd_achievement, monthly_projected_achievement
FROM tmp_brand_target_sales_master_details;


COMMIT;

