BEGIN;
DROP TABLE IF EXISTS tmp_consolidated_sku_stock;
CREATE TEMP TABLE tmp_consolidated_sku_stock(
    sku VARCHAR(80) NULL,
    report_at_AST TIMESTAMP NULL,
    nav_warehouse_sellable DECIMAL(38,0) NULL,
    nav_warehouse_not_sellable DECIMAL(38,0) NULL,
    nav_others_not_sellable DECIMAL(38,0) NULL,
    nav2crs_total DECIMAL(38,0) NULL,
    crs_reserved DECIMAL(38,0) NULL,
    crs_available DECIMAL(38,0) NULL,
    crs_actual_available DECIMAL(38,0) NULL,
    crs_force_soldout SMALLINT NULL,
    crs_total DECIMAL(38,0) NULL,
    ofs_not_picked_or_cancelled DECIMAL(38,0) NULL,
    wh_reserved BIGINT NULL,
    nav_grn_pending_putaway BIGINT NULL,
    nav_return_pending_putaway BIGINT NULL,
    nav_bin_to_bin_movement BIGINT NULL,
    nav_total DECIMAL(38,0) NULL,
    crs_available_diff DECIMAL(38,0) NULL,
    crs_reserved_diff DECIMAL(38,0) NULL,
    soh DECIMAL(38,0) NULL
);

copy tmp_consolidated_sku_stock from '{{S3PATH}}'
iam_role 'arn:aws:iam::652586300051:role/Redshift-athena-S3-readonly'
delimiter '\t'
region 'eu-west-1' 
acceptinvchars
CSV 
GZIP 
emptyasnull
blanksasnull
NULL AS 'null';

CREATE TABLE IF NOT EXISTS aoi.consolidated_sku_stock (
    sku VARCHAR(80) NULL,
    report_at_AST TIMESTAMP NULL,
    nav_warehouse_sellable DECIMAL(38,0) NULL,
    nav_warehouse_not_sellable DECIMAL(38,0) NULL,
    nav_others_not_sellable DECIMAL(38,0) NULL,
    nav2crs_total DECIMAL(38,0) NULL,
    crs_reserved DECIMAL(38,0) NULL,
    crs_available DECIMAL(38,0) NULL,
    crs_actual_available DECIMAL(38,0) NULL,
    crs_force_soldout SMALLINT NULL,
    crs_total DECIMAL(38,0) NULL,
    ofs_not_picked_or_cancelled DECIMAL(38,0) NULL,
    wh_reserved BIGINT NULL,
    nav_grn_pending_putaway BIGINT NULL,
    nav_return_pending_putaway BIGINT NULL,
    nav_bin_to_bin_movement BIGINT NULL,
    nav_total DECIMAL(38,0) NULL,
    crs_available_diff DECIMAL(38,0) NULL,
    crs_reserved_diff DECIMAL(38,0) NULL,
    soh DECIMAL(38,0) NULL
);

DELETE FROM aoi.consolidated_sku_stock WHERE 1=1;

INSERT INTO aoi.consolidated_sku_stock
SELECT sku,report_at_AST,nav_warehouse_sellable,nav_warehouse_not_sellable,nav_others_not_sellable,nav2crs_total,crs_reserved,crs_available,crs_actual_available,crs_force_soldout,crs_total,ofs_not_picked_or_cancelled,wh_reserved,nav_grn_pending_putaway,nav_return_pending_putaway,nav_bin_to_bin_movement,nav_total,crs_available_diff,crs_reserved_diff,soh
FROM tmp_consolidated_sku_stock;


COMMIT;
