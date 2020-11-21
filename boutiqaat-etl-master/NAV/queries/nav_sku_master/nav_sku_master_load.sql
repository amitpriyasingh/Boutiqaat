BEGIN;
DROP TABLE IF EXISTS tmp_nav_sku_master;
CREATE TEMP TABLE tmp_nav_sku_master(
    sku VARCHAR(20),
    sku_name VARCHAR(200),
    bar_code VARCHAR(30),
    brand_code VARCHAR(20),
    supplier_color VARCHAR(50),
    department VARCHAR(50),
    category_manager  VARCHAR(50),
    brand VARCHAR(101),
    category1 VARCHAR(40),
    category2 VARCHAR(40),
    category3 VARCHAR(40),
    category4 VARCHAR(40),
    first_selling_price decimal(38,20),
    last_selling_price decimal(38,20),
    supplier_cost decimal(38,20),
    landed_cost decimal(38,20),
    first_price_entry_date timestamp,
    last_price_entry_date timestamp,
    last_item_cost decimal(38,20),
    shipping_cost_per_unit decimal(38,20),
    last_item_cost_currency VARCHAR(20),
    vendor_no VARCHAR(4000),
    vendor_name VARCHAR(4000),
    country_code VARCHAR(4000),
    contract_type VARCHAR(4000),
    payment_term_code VARCHAR(4000),
    vendor_item_no VARCHAR(4000),
    first_grn_date timestamp,
    last_grn_date timestamp,
    grn_qty_yesterday decimal(38,20),
    grn_qty_last_2nd_day decimal(38,20),
    grn_qty_last_3rd_day decimal(38,20),
    grn_qty_last_4th_day decimal(38,20),
    grn_qty_last_5th_day decimal(38,20),
    grn_qty_last_6th_day decimal(38,20),
    grn_qty_last_7th_day decimal(38,20),
    grn_qty_2020 decimal(38,20),
    grn_value_2020 decimal(38,6),
    sku_avg_cost_2020 decimal(38,6),
    grn_qty_2019 decimal(38,20),
    grn_value_2019 decimal(38,6),
    sku_avg_cost_2019 decimal(38,6),
    grn_qty_2018 decimal(38,20),
    grn_value_2018 decimal(38,6),
    sku_avg_cost_2018 decimal(38,6),
    grn_qty_2017 decimal(38,20),
    grn_value_2017 decimal(38,6),
    sku_avg_cost_2017 decimal(38,6),
    grn_qty_2016 decimal(38,20),
    grn_value_2016 decimal(38,6),
    sku_avg_cost_2016 decimal(38,6),
    grn_qty_2015 decimal(38,20),
    grn_value_2015 decimal(38,6),
    sku_avg_cost_2015 decimal(38,6),
    last_putaway_date timestamp 
 );

copy tmp_nav_sku_master from '{{S3PATH}}'
iam_role 'arn:aws:iam::652586300051:role/Redshift-athena-S3-readonly'
delimiter '\t'
region 'eu-west-1'
acceptinvchars
CSV 
GZIP 
emptyasnull
blanksasnull
NULL AS 'null';

DROP TABLE IF EXISTS NAV.nav_sku_master;
CREATE TABLE IF NOT EXISTS NAV.nav_sku_master (
    sku VARCHAR(20) NULL,
    sku_name VARCHAR(200) NULL,
    bar_code VARCHAR(30) NULL,
    brand_code VARCHAR(20) NULL,
    supplier_color VARCHAR(50)  NULL,
    department VARCHAR(50) NULL,
    category_manager VARCHAR(50) NULL,
    brand VARCHAR(101)  NULL,
    category1 VARCHAR(40) NULL,
    category2 VARCHAR(40) NULL,
    category3 VARCHAR(40) NULL,
    category4 VARCHAR(40) NULL,
    first_selling_price decimal(38,20) NULL,
    last_selling_price decimal(38,20) NULL,
    supplier_cost decimal(38,20) NULL,
    landed_cost decimal(38,20) NULL,
    first_price_entry_date timestamp NULL,
    last_price_entry_date timestamp NULL,
    last_item_cost decimal(38,20) NULL,
    shipping_cost_per_unit decimal(38,20) NULL,
    last_item_cost_currency VARCHAR(20)  NULL,
    vendor_no VARCHAR(4000)  NULL,
    vendor_name VARCHAR(4000)  NULL,
    country_code VARCHAR(4000)  NULL,
    contract_type VARCHAR(4000) NULL,
    payment_term_code VARCHAR(4000)  NULL,
    vendor_item_no VARCHAR(4000)  NULL,
    first_grn_date timestamp NULL,
    last_grn_date timestamp NULL,
    grn_qty_yesterday decimal(38,20),
    grn_qty_last_2nd_day decimal(38,20),
    grn_qty_last_3rd_day decimal(38,20),
    grn_qty_last_4th_day decimal(38,20),
    grn_qty_last_5th_day decimal(38,20),
    grn_qty_last_6th_day decimal(38,20),
    grn_qty_last_7th_day decimal(38,20),
    grn_qty_2020 decimal(38,20) NULL,
    grn_value_2020 decimal(38,6) NULL,
    sku_avg_cost_2020 decimal(38,6) NULL,
    grn_qty_2019 decimal(38,20) NULL,
    grn_value_2019 decimal(38,6) NULL,
    sku_avg_cost_2019 decimal(38,6) NULL,
    grn_qty_2018 decimal(38,20) NULL,
    grn_value_2018 decimal(38,6) NULL,
    sku_avg_cost_2018 decimal(38,6) NULL,
    grn_qty_2017 decimal(38,20) NULL,
    grn_value_2017 decimal(38,6) NULL,
    sku_avg_cost_2017 decimal(38,6) NULL,
    grn_qty_2016 decimal(38,20) NULL,
    grn_value_2016 decimal(38,6) NULL,
    sku_avg_cost_2016 decimal(38,6) NULL,
    grn_qty_2015 decimal(38,20) NULL,
    grn_value_2015 decimal(38,6) NULL,
    sku_avg_cost_2015 decimal(38,6) NULL,
    last_putaway_date timestamp NULL
);

INSERT INTO NAV.nav_sku_master
SELECT sku,sku_name,bar_code,brand_code,supplier_color,department,category_manager,brand,category1,category2,category3,category4,first_selling_price,last_selling_price,supplier_cost,landed_cost,first_price_entry_date,last_price_entry_date,last_item_cost,shipping_cost_per_unit,last_item_cost_currency,vendor_no,vendor_name,country_code,contract_type,payment_term_code,vendor_item_no,first_grn_date,last_grn_date,grn_qty_yesterday,grn_qty_last_2nd_day,grn_qty_last_3rd_day,grn_qty_last_4th_day,grn_qty_last_5th_day,grn_qty_last_6th_day,grn_qty_last_7th_day,grn_qty_2020,grn_value_2020,sku_avg_cost_2020,grn_qty_2019,grn_value_2019,sku_avg_cost_2019,grn_qty_2018,grn_value_2018,sku_avg_cost_2018,grn_qty_2017,grn_value_2017,sku_avg_cost_2017,grn_qty_2016,grn_value_2016,sku_avg_cost_2016,grn_qty_2015,grn_value_2015,sku_avg_cost_2015,last_putaway_date
FROM tmp_nav_sku_master
group by sku,sku_name,bar_code,brand_code,supplier_color,department,category_manager,brand,category1,category2,category3,category4,first_selling_price,last_selling_price,supplier_cost,landed_cost,first_price_entry_date,last_price_entry_date,last_item_cost,shipping_cost_per_unit,last_item_cost_currency,vendor_no,vendor_name,country_code,contract_type,payment_term_code,vendor_item_no,first_grn_date,last_grn_date,grn_qty_yesterday,grn_qty_last_2nd_day,grn_qty_last_3rd_day,grn_qty_last_4th_day,grn_qty_last_5th_day,grn_qty_last_6th_day,grn_qty_last_7th_day,grn_qty_2020,grn_value_2020,sku_avg_cost_2020,grn_qty_2019,grn_value_2019,sku_avg_cost_2019,grn_qty_2018,grn_value_2018,sku_avg_cost_2018,grn_qty_2017,grn_value_2017,sku_avg_cost_2017,grn_qty_2016,grn_value_2016,sku_avg_cost_2016,grn_qty_2015,grn_value_2015,sku_avg_cost_2015,last_putaway_date;
COMMIT;
