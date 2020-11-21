BEGIN;
DROP TABLE IF EXISTS tmp_item_vendor_discount;
CREATE TEMP TABLE tmp_item_vendor_discount(
	ts VARCHAR(40),
	item_no VARCHAR(40),
	vendor_no VARCHAR(40),
	start_date timestamp,
	brand VARCHAR(40),
	item_category_code_1 VARCHAR(40),
	item_category_code_2 VARCHAR(40),
	item_category_code_3 VARCHAR(40),
	item_category_code_4 VARCHAR(40),
	entry_no INTEGER,
	retail_price decimal(38,20),
	discount decimal(38,20),
	cost decimal(38,20),
	cost_price_calculation INTEGER,
	vendor_discount_type INTEGER,
	blocked SMALLINT,
	vendor_disc_line_no INTEGER
);

copy tmp_item_vendor_discount from '{{S3PATH}}'
iam_role 'arn:aws:iam::652586300051:role/Redshift-athena-S3-readonly'
delimiter '\t'
region 'eu-west-1' 
acceptinvchars
CSV 
GZIP 
emptyasnull
blanksasnull
NULL AS 'null';


DROP TABLE IF EXISTS NAV.item_vendor_discount;
CREATE TABLE NAV.item_vendor_discount (	
	ts VARCHAR(40) NULL ENCODE LZO,
	item_no VARCHAR(40) NULL ENCODE LZO,
	vendor_no VARCHAR(40) NULL ENCODE LZO,
	start_date timestamp NULL ENCODE LZO,
	brand VARCHAR(40) NULL ENCODE LZO,
	item_category_code_1 VARCHAR(40) NULL ENCODE LZO,
	item_category_code_2 VARCHAR(40) NULL ENCODE LZO,
	item_category_code_3 VARCHAR(40) NULL ENCODE LZO,
	item_category_code_4 VARCHAR(40) NULL ENCODE LZO,
	entry_no INTEGER NULL ENCODE DELTA,
	retail_price decimal(38,20) NULL ENCODE LZO,
	discount decimal(38,20) NULL ENCODE LZO,
	cost decimal(38,20) NULL ENCODE LZO,
	cost_price_calculation INTEGER NULL ENCODE DELTA,
	vendor_discount_type INTEGER NULL ENCODE DELTA,
	blocked SMALLINT NULL ENCODE DELTA,
	vendor_disc_line_no INTEGER NULL ENCODE DELTA
);


INSERT INTO NAV.item_vendor_discount 
SELECT ts, item_no, vendor_no, start_date, brand, item_category_code_1, item_category_code_2, item_category_code_3, item_category_code_4, entry_no, retail_price, discount, cost, cost_price_calculation, vendor_discount_type, blocked, vendor_disc_line_no
FROM tmp_item_vendor_discount;
COMMIT;