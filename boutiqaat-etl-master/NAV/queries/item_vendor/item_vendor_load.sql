BEGIN;
DROP TABLE IF EXISTS tmp_item_vendor;
CREATE TEMP TABLE tmp_item_vendor(
	ts VARCHAR(40),
	vendor_no VARCHAR(40),
	item_no VARCHAR(40),
	variant_code VARCHAR(20),
	lead_time_calculation varchar(32),
	vendor_item_no VARCHAR(40),
	start_date timestamp,
	end_date timestamp,
	status INTEGER,
	modified_by VARCHAR(40),
	purchase_price decimal(38,20),
	item_category_code_1 VARCHAR(40),
	item_category_code_2 VARCHAR(40),
	item_category_code_3 VARCHAR(40),
	item_category_code_4 VARCHAR(40),
	brand VARCHAR(40),
	name_in_english VARCHAR(200),
	name_in_arabic VARCHAR(500)
);

copy tmp_item_vendor from '{{S3PATH}}'
iam_role 'arn:aws:iam::652586300051:role/Redshift-athena-S3-readonly'
delimiter '\t'
region 'eu-west-1' 
acceptinvchars
CSV 
GZIP 
emptyasnull
blanksasnull
NULL AS 'null';


DROP TABLE IF EXISTS NAV.item_vendor;
CREATE TABLE NAV.item_vendor (	
	ts VARCHAR(40) NULL ENCODE LZO,
	vendor_no VARCHAR(40) NULL ENCODE LZO,
	item_no VARCHAR(40) NULL ENCODE LZO DISTKEY SORTKEY,
	variant_code VARCHAR(20) NULL ENCODE LZO,
	lead_time_calculation varchar(32) NULL ENCODE LZO,
	vendor_item_no VARCHAR(40) NULL ENCODE LZO,
	start_date timestamp NULL ENCODE LZO,
	end_date timestamp NULL ENCODE LZO,
	status INTEGER NULL ENCODE DELTA,
	modified_by VARCHAR(40) NULL ENCODE LZO,
	purchase_price decimal(38,20) NULL ENCODE LZO,
	item_category_code_1 VARCHAR(40) NULL ENCODE LZO,
	item_category_code_2 VARCHAR(40) NULL ENCODE LZO,
	item_category_code_3 VARCHAR(40) NULL ENCODE LZO,
	item_category_code_4 VARCHAR(40) NULL ENCODE LZO,
	brand VARCHAR(40) NULL ENCODE LZO,
	name_in_english VARCHAR(200) NULL ENCODE LZO,
	name_in_arabic VARCHAR(500) NULL ENCODE LZO
);


INSERT INTO NAV.item_vendor
SELECT ts, vendor_no, item_no, variant_code, lead_time_calculation, vendor_item_no, start_date, end_date, status, modified_by, purchase_price, item_category_code_1, item_category_code_2, item_category_code_3, item_category_code_4, brand, name_in_english, name_in_arabic
FROM tmp_item_vendor;
COMMIT;