BEGIN;
DROP TABLE IF EXISTS tmp_nav_sku_cost;
CREATE TEMP TABLE tmp_nav_sku_cost(
    sku VARCHAR(20),
    vendor_count INTEGER,
    max_posting_date TIMESTAMP,
    last_item_cost_kwd DECIMAL(38,20),
    last_item_cost DECIMAL(38,20),
    last_item_cost_currency VARCHAR(10)
 );

copy tmp_nav_sku_cost from '{{S3PATH}}'
iam_role 'arn:aws:iam::652586300051:role/Redshift-athena-S3-readonly'
delimiter '\t'
region 'eu-west-1'
acceptinvchars
CSV 
GZIP 
emptyasnull
blanksasnull
NULL AS 'null';

CREATE TABLE IF NOT EXISTS NAV.nav_sku_cost (
    sku VARCHAR(20),
    vendor_count INTEGER,
    max_posting_date TIMESTAMP,
    last_item_cost_kwd DECIMAL(38,20),
    last_item_cost DECIMAL(38,20),
    last_item_cost_currency VARCHAR(10)
);

DELETE FROM NAV.nav_sku_cost WHERE 1=1;

INSERT INTO NAV.nav_sku_cost
SELECT sku, vendor_count, max_posting_date, last_item_cost_kwd, last_item_cost, last_item_cost_currency
FROM tmp_nav_sku_cost;
COMMIT;
