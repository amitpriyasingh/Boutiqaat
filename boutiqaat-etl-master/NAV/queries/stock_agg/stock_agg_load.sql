BEGIN;
DROP TABLE IF EXISTS tmp_stock_agg;
CREATE TEMP TABLE tmp_stock_agg(
    sku nvarchar(20),
    putpick_qty decimal(38,20),
    qty_movement integer,
    qty_return_qc_pass_and_grn_pending_putaway decimal(38,20) ,
    qty_stagemovebin decimal(38,20),
    total_sellable_qty decimal(38,20),
    nav2crs_total integer,
    wh_entry integer,
    wh_jn_line integer,
    wh_activity_line integer,
    non_sellable_qty_ccstagebin decimal(38,20),
    non_sellable_qty_damaged_inventory decimal(38,20),
    non_sellable_qty_exp_inventory decimal(38,20),
    warehouse_reserved_qty integer,
    non_sellable_online_store_qty decimal(38,20),
    non_sellable_mishref_qty decimal(38,20),
    non_sellable_intransit_sys_qty decimal(38,20),
    non_sellable_content_abzak_qty decimal(38,20),
    non_sellable_showroom_6th_floor_qty decimal(38,20),
    non_sellable_qatar_exhibition_qty decimal(38,20),
    non_sellable_old_return_solv_qty decimal(38,20),
    non_sellable_mgmt_qty decimal(38,20),
    non_sellable_kwpurch_qty decimal(38,20),
    non_sellable_showroom_m1_floor_qty decimal(38,20),
    non_sellable_abrar_qty decimal(38,20),
    non_sellable_marketing_gift_qty decimal(38,20),
    non_sellable_new_showroom_qty decimal(38,20),
    non_sellable_dubai_samp_qty decimal(38,20),
    non_sellable_tv_mashour_qty decimal(38,20),
    non_sellable_other_offline_qty decimal(38,20),
    non_sellable_designer_hamdan_qty decimal(38,20),
    non_sellable_sample_qty decimal(38,20),
    non_sellable_other_qty decimal(38,20),
    toal_nav_non_sellable decimal(38,20),
    full_pending_open_po_qty decimal(38,20),
    partially_pending_open_po_qty decimal(38,20),
    partial_pending_open_po_total_qty decimal(38,20),
    partial_pending_open_po_received_qty decimal(38,20),
    soh decimal(38,20),
    stock_refreshed_datetime TIMESTAMP
);

copy tmp_stock_agg from '{{S3PATH}}'
iam_role 'arn:aws:iam::652586300051:role/Redshift-athena-S3-readonly'
delimiter '\t'
region 'eu-west-1'
acceptinvchars
CSV 
GZIP 
emptyasnull
blanksasnull
NULL AS 'null';

DROP TABLE IF EXISTS NAV.stock_agg;
CREATE TABLE IF NOT EXISTS NAV.stock_agg (
    sku nvarchar(20)  NOT NULL,
    putpick_qty decimal(38,20) NULL,
    qty_movement integer NULL,
    qty_return_qc_pass_and_grn_pending_putaway decimal(38,20) NULL,
    qty_stagemovebin decimal(38,20) NULL,
    total_sellable_qty decimal(38,20) NULL,
    nav2crs_total integer NULL,
    wh_entry integer NULL,
    wh_jn_line integer NULL,
    wh_activity_line integer NULL,
    non_sellable_qty_ccstagebin decimal(38,20) NULL,
    non_sellable_qty_damaged_inventory decimal(38,20) NULL,
    non_sellable_qty_exp_inventory decimal(38,20) NULL,
    warehouse_reserved_qty integer NULL,
    non_sellable_online_store_qty decimal(38,20) NULL,
    non_sellable_mishref_qty decimal(38,20) NULL,
    non_sellable_intransit_sys_qty decimal(38,20) NULL,
    non_sellable_content_abzak_qty decimal(38,20) NULL,
    non_sellable_showroom_6th_floor_qty decimal(38,20) NULL,
    non_sellable_qatar_exhibition_qty decimal(38,20) NULL,
    non_sellable_old_return_solv_qty decimal(38,20) NULL,
    non_sellable_mgmt_qty decimal(38,20) NULL,
    non_sellable_kwpurch_qty decimal(38,20) NULL,
    non_sellable_showroom_m1_floor_qty decimal(38,20) NULL,
    non_sellable_abrar_qty decimal(38,20) NULL,
    non_sellable_marketing_gift_qty decimal(38,20) NULL,
    non_sellable_new_showroom_qty decimal(38,20) NULL,
    non_sellable_dubai_samp_qty decimal(38,20) NULL,
    non_sellable_tv_mashour_qty decimal(38,20) NULL,
    non_sellable_other_offline_qty decimal(38,20) NULL,
    non_sellable_designer_hamdan_qty decimal(38,20) NULL,
    non_sellable_sample_qty decimal(38,20) NULL,
    non_sellable_other_qty decimal(38,20) NULL,
    toal_nav_non_sellable decimal(38,20) NULL,
    full_pending_open_po_qty decimal(38,20) NULL,
    partially_pending_open_po_qty decimal(38,20) NULL,
    partial_pending_open_po_total_qty decimal(38,20) NULL,
    partial_pending_open_po_received_qty decimal(38,20) NULL,
    soh decimal(38,20) NULL,
    stock_refreshed_datetime TIMESTAMP NULL
);

INSERT INTO NAV.stock_agg
SELECT *
FROM tmp_stock_agg;
COMMIT;
