BEGIN;
DROP TABLE IF EXISTS tmp_aoi_soh_report;
CREATE TEMP TABLE tmp_aoi_soh_report(
    num DECIMAL(10,3),
    location VARCHAR(80),
    supplier VARCHAR(80),
    supplier_item_no VARCHAR(80),
    vendor_code VARCHAR(30),
    brand VARCHAR(80),
    brand_code VARCHAR(80),
    sku VARCHAR(30),
    sku2 VARCHAR(30),
    barcode VARCHAR(30),
    category1 VARCHAR(80),
    category2 VARCHAR(80),
    category3 VARCHAR(80),
    category4 VARCHAR(80),
    sku_name VARCHAR(255),
    retail_price DECIMAL(10,3),
    cost_price DECIMAL(10,3),
    soh BIGINT,
    open_po_total_qty BIGINT,
    open_po_pending_receipt_qty BIGINT,
    open_po_partially_received_qty BIGINT,
    open_po_pending_cancellation_qty BIGINT,
    reserved_qty BIGINT,
    first_grn_date TIMESTAMP,
    last_grn_date TIMESTAMP,
    payment_term_code VARCHAR(30),
    country VARCHAR(30),
    stock_entry_synched_at TIMESTAMP,
    sale_entry_synched_at TIMESTAMP,
    report_date TIMESTAMP,
    sold_qty_today INTEGER,
    sold_qty_yesterday INTEGER,
    sold_qty_7days INTEGER,
    sold_qty_14days INTEGER,
    sold_qty_mtd INTEGER,
    sold_qty_m1 INTEGER,
    sold_qty_m2 INTEGER,
    sold_qty_m3 INTEGER,
    sold_qty_m4 INTEGER,
    sold_qty_m5 INTEGER,
    sold_qty_lifetime BIGINT,
    sellable_quantity BIGINT,
    not_sellable_quantity BIGINT,
    nav_total_quantity BIGINT,
    department VARCHAR(100),
    category_manager VARCHAR(200),
    hod VARCHAR(30)
);

copy tmp_aoi_soh_report from '{{S3PATH}}'
iam_role 'arn:aws:iam::652586300051:role/Redshift-athena-S3-readonly'
delimiter '\t'
region 'eu-west-1' 
acceptinvchars
CSV 
GZIP 
emptyasnull
blanksasnull
NULL AS 'null';

CREATE TABLE IF NOT EXISTS aoi.soh_report (
    location VARCHAR(80) NULL ENCODE LZO,
    supplier VARCHAR(80) NULL ENCODE LZO,
    supplier_item_no VARCHAR(80) NULL ENCODE LZO,
    vendor_code VARCHAR(30) NULL ENCODE LZO,
    brand VARCHAR(80) NULL ENCODE LZO,
    brand_code VARCHAR(80) NULL ENCODE LZO,
    sku VARCHAR(30) NOT NULL ENCODE LZO PRIMARY KEY DISTKEY SORTKEY,
    sku2 VARCHAR(30) NULL ENCODE LZO,
    barcode VARCHAR(30) NULL ENCODE LZO,
    category1 VARCHAR(80) NULL ENCODE LZO,
    category2 VARCHAR(80) NULL ENCODE LZO,
    category3 VARCHAR(80) NULL ENCODE LZO,
    category4 VARCHAR(80) NULL ENCODE LZO,
    sku_name VARCHAR(255) NULL ENCODE LZO,
    retail_price DECIMAL(10,3) NULL ENCODE LZO,
    cost_price DECIMAL(10,3) NULL ENCODE LZO,
    soh BIGINT NULL ENCODE LZO,
    open_po_total_qty BIGINT NULL ENCODE LZO,
    open_po_pending_receipt_qty BIGINT NULL ENCODE LZO,
    open_po_partially_received_qty BIGINT NULL ENCODE LZO,
    open_po_pending_cancellation_qty BIGINT NULL ENCODE LZO,
    reserved_qty BIGINT NULL ENCODE LZO,
    first_grn_date TIMESTAMP NULL ENCODE LZO,
    last_grn_date TIMESTAMP NULL ENCODE LZO,
    payment_term_code VARCHAR(30) NULL ENCODE LZO,
    country VARCHAR(30) NULL ENCODE LZO,
    stock_entry_synched_at TIMESTAMP NULL ENCODE LZO,
    sale_entry_synched_at TIMESTAMP NULL ENCODE LZO,
    report_date TIMESTAMP NOT NULL ENCODE LZO,
    sold_qty_today INTEGER NULL ENCODE LZO,
    sold_qty_yesterday INTEGER NULL ENCODE LZO,
    sold_qty_7days INTEGER NULL ENCODE LZO,
    sold_qty_14days INTEGER NULL ENCODE LZO,
    sold_qty_mtd INTEGER NULL ENCODE LZO,
    sold_qty_m1 INTEGER NULL ENCODE LZO,
    sold_qty_m2 INTEGER NULL ENCODE LZO,
    sold_qty_m3 INTEGER NULL ENCODE LZO,
    sold_qty_m4 INTEGER NULL ENCODE LZO,
    sold_qty_m5 INTEGER NULL ENCODE LZO,
    sold_qty_lifetime BIGINT NULL ENCODE LZO,
    sellable_quantity BIGINT NULL ENCODE LZO,
    not_sellable_quantity BIGINT NULL ENCODE LZO,
    nav_total_quantity BIGINT NULL ENCODE LZO,
    department VARCHAR(100) NULL ENCODE LZO,
    category_manager VARCHAR(200) NULL ENCODE LZO,
    hod  VARCHAR(200) NULL ENCODE LZO
);

DELETE FROM aoi.soh_report WHERE 1=1;

INSERT INTO aoi.soh_report
SELECT location,supplier,supplier_item_no,vendor_code,brand,brand_code,sku,sku2,barcode,category1,category2,category3,category4,sku_name,retail_price,cost_price,soh,open_po_total_qty,open_po_pending_receipt_qty,open_po_partially_received_qty,open_po_pending_cancellation_qty,reserved_qty,first_grn_date,last_grn_date,payment_term_code,country,stock_entry_synched_at,sale_entry_synched_at,report_date,sold_qty_today,sold_qty_yesterday,sold_qty_7days,sold_qty_14days,sold_qty_mtd,sold_qty_m1,sold_qty_m2,sold_qty_m3,sold_qty_m4,sold_qty_m5,sold_qty_lifetime,sellable_quantity,not_sellable_quantity,nav_total_quantity,department,category_manager,hod
FROM tmp_aoi_soh_report;


COMMIT;

