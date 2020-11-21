BEGIN;
DROP TABLE IF EXISTS tmp_inventory_health;
CREATE TEMP TABLE tmp_inventory_health(
    sku VARCHAR(30),
    barcode VARCHAR(30),
    sku_name VARCHAR(255),
    brand VARCHAR(80),
    department VARCHAR(100),
    category1 VARCHAR(80),
    category2 VARCHAR(80),
    vendor_code VARCHAR(30),
    supplier VARCHAR(80),
    first_grn_date TIMESTAMP,
    first_order_date VARCHAR(10),
    last_order_date VARCHAR(10),
    soh INTEGER,
    open_po_qty BIGINT,
    crs_available_qty DECIMAL(32,0),
    first_sale_in_60_days VARCHAR(3),
    days_sold_60days BIGINT,
    sold_qty_07days DECIMAL(25,0),
    sold_qty_14days DECIMAL(25,0),
    sold_qty_30_days DECIMAL(25,0),
    sold_qty_60_days DECIMAL(25,0),
    sold_qty_90_days DECIMAL(25,0),
    sold_qty_180_days DECIMAL(25,0),
    lifetime_sold_qty DECIMAL(25,0),
    gift_qty DECIMAL(25,0),
    stock_cover_60days_sale_basis VARCHAR(67),
    stock_cover_60days_including_open_po VARCHAR(67),
    stock_cover_flag VARCHAR(12),
    stock_cover_flag_including_open_po VARCHAR(12),
    inventory_category VARCHAR(13),
    firt_grn_to_first_sale_days INTEGER,
    sku_live_status VARCHAR(8),
    events_count_30days BIGINT,
    events_count_60days BIGINT,
    events_count_90days BIGINT
);

copy tmp_inventory_health from '{{S3PATH}}'
iam_role 'arn:aws:iam::652586300051:role/Redshift-athena-S3-readonly'
delimiter '\t'
region 'eu-west-1' 
acceptinvchars
CSV 
GZIP 
emptyasnull
blanksasnull
NULL AS 'null';

CREATE TABLE IF NOT EXISTS aoi.inventory_health (
    sku VARCHAR(30) NOT NULL ENCODE LZO DISTKEY SORTKEY,
    barcode VARCHAR(30) NULL ENCODE LZO,
    sku_name VARCHAR(255) NULL ENCODE LZO,
    brand VARCHAR(80) NULL ENCODE LZO,
    department VARCHAR(100) NULL ENCODE LZO,
    category1 VARCHAR(80) NULL ENCODE LZO,
    category2 VARCHAR(80) NULL ENCODE LZO,
    vendor_code VARCHAR(30) NULL ENCODE LZO,
    supplier VARCHAR(80) NULL ENCODE LZO,
    first_grn_date TIMESTAMP NULL ENCODE LZO,
    first_order_date VARCHAR(10) NULL ENCODE LZO,
    last_order_date VARCHAR(10) NULL ENCODE LZO,
    soh INTEGER NULL ENCODE LZO,
    open_po_qty BIGINT NULL ENCODE LZO,
    crs_available_qty DECIMAL(32,0) NULL ENCODE LZO,
    first_sale_in_60_days VARCHAR(3) NOT NULL ENCODE LZO,
    days_sold_60days BIGINT NULL ENCODE LZO,
    sold_qty_07days DECIMAL(25,0) NULL ENCODE LZO,
    sold_qty_14days DECIMAL(25,0) NULL ENCODE LZO,
    sold_qty_30_days DECIMAL(25,0) NULL ENCODE LZO,
    sold_qty_60_days DECIMAL(25,0) NULL ENCODE LZO,
    sold_qty_90_days DECIMAL(25,0) NULL ENCODE LZO,
    sold_qty_180_days DECIMAL(25,0) NULL ENCODE LZO,
    lifetime_sold_qty DECIMAL(25,0) NULL ENCODE LZO,
    gift_qty DECIMAL(25,0) NULL ENCODE LZO,
    stock_cover_60days_sale_basis VARCHAR(67) NULL ENCODE LZO,
    stock_cover_60days_including_open_po VARCHAR(67) NULL ENCODE LZO,
    stock_cover_flag VARCHAR(12) NOT NULL ENCODE LZO,
    stock_cover_flag_including_open_po VARCHAR(12) NOT NULL ENCODE LZO,
    inventory_category VARCHAR(13) NOT NULL ENCODE LZO,
    firt_grn_to_first_sale_days INTEGER NULL ENCODE LZO,
    sku_live_status VARCHAR(8) NOT NULL ENCODE LZO,
    events_count_30days BIGINT NULL ENCODE LZO,
    events_count_60days BIGINT NULL ENCODE LZO,
    events_count_90days BIGINT NULL ENCODE LZO
);

DELETE FROM aoi.inventory_health WHERE 1=1;

INSERT INTO aoi.inventory_health
SELECT * FROM tmp_inventory_health;


COMMIT;
