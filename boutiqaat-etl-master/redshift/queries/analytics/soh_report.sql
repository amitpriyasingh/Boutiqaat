BEGIN;
DROP TABLE IF EXISTS analytics.soh_report;
SELECT * INTO analytics.soh_report FROM
(SELECT 
    nsm.sku,
    nsm.sku_name AS nav_sku_name,
    msm.sku_name AS magento_sku_name,
    msm.sku_gender as gender,
    msm.parent_sku as config_sku,
    msm.size,
    msm.color,
    CASE
        WHEN msm.celebrity_exclusive IS NOT NULL THEN 
        CASE
            WHEN msm.celebrity_exclusive = 1 THEN 'Yes'::character varying
            ELSE 'No'::character varying
        END
        ELSE NULL::character varying
    END AS celebrity_exclusive, 
    CASE
        WHEN msm.boutiqaat_exclusive IS NOT NULL THEN 
        CASE
            WHEN msm.boutiqaat_exclusive::text = 1::character varying::text THEN 'Yes'::character varying
            ELSE 'No'::character varying
        END
        ELSE NULL::character varying
    END AS boutiqaat_exclusive, 
    CASE
        WHEN msm.special_price = 0::numeric::numeric(20,6) THEN NULL::numeric::numeric(18,0)
        ELSE msm.special_price
    END AS special_price, 
    msm.first_online_date as enable_date, 
    nsm.bar_code, 
    nsm.brand_code, 
    nsm.brand, 
    nsm.category1, 
    nsm.category2, 
    nsm.category3, 
    nsm.category4, 
    nsm.first_selling_price, 
    nsm.last_selling_price, 
    nsm.first_price_entry_date, 
    nsm.last_price_entry_date, 
    nsm.last_item_cost, 
    nsm.shipping_cost_per_unit, 
    nsm.last_item_cost_currency, 
    nsm.supplier_cost, 
    nsm.landed_cost, 
    nsm.vendor_no, 
    nsm.vendor_name, 
    nsm.country_code, 
    nsm.contract_type, 
    nsm.payment_term_code, 
    nsm.vendor_item_no, 
    nsm.first_grn_date, 
    nsm.last_grn_date, 
    nsm.grn_qty_yesterday,
    nsm.grn_qty_last_2nd_day,
    nsm.grn_qty_last_3rd_day,
    nsm.grn_qty_last_4th_day,
    nsm.grn_qty_last_5th_day,
    nsm.grn_qty_last_6th_day,
    nsm.grn_qty_last_7th_day,
    nsm.grn_qty_2020, 
    nsm.grn_value_2020, 
    nsm.sku_avg_cost_2020, 
    nsm.grn_qty_2019, 
    nsm.grn_value_2019, 
    nsm.sku_avg_cost_2019, 
    nsm.grn_qty_2018, 
    nsm.grn_value_2018, 
    nsm.sku_avg_cost_2018, 
    nsm.grn_qty_2017, 
    nsm.grn_value_2017, 
    nsm.sku_avg_cost_2017, 
    nsm.grn_qty_2016, 
    nsm.grn_value_2016, 
    nsm.sku_avg_cost_2016, 
    nsm.grn_qty_2015, 
    nsm.grn_value_2015, 
    nsm.sku_avg_cost_2015, 
    COALESCE(nsm.grn_qty_2020, 0::numeric) + COALESCE(nsm.grn_qty_2019, 0::numeric) 
    + COALESCE(nsm.grn_qty_2018, 0::numeric) + COALESCE(nsm.grn_qty_2017, 0::numeric) 
    + COALESCE(nsm.grn_qty_2016, 0::numeric) + COALESCE(nsm.grn_qty_2015, 0::numeric) AS total_grn_qty, 
    COALESCE(nsm.grn_value_2020, 0::numeric) + COALESCE(nsm.grn_value_2019, 0::numeric) 
    + COALESCE(nsm.grn_value_2018, 0::numeric) + COALESCE(nsm.grn_value_2017, 0::numeric) 
    + COALESCE(nsm.grn_value_2016, 0::numeric) + COALESCE(nsm.grn_value_2015, 0::numeric) AS total_grn_value, 
    nsm.last_putaway_date, 
    sa.putpick_qty, 
    sa.qty_movement, 
    sa.qty_return_qc_pass_and_grn_pending_putaway, 
    sa.qty_stagemovebin, 
    sa.total_sellable_qty, 
    sa.nav2crs_total, 
    sa.wh_entry, 
    sa.wh_jn_line, 
    sa.wh_activity_line, 
    sa.non_sellable_qty_ccstagebin, 
    sa.non_sellable_qty_damaged_inventory, 
    sa.non_sellable_qty_exp_inventory, 
    sa.warehouse_reserved_qty, 
    crs.crs_available, 
    crs.crs_reserved, 
    crs.ofs_not_picked_or_cancelled, 
    sa.non_sellable_online_store_qty, 
    sa.non_sellable_mishref_qty, 
    sa.non_sellable_intransit_sys_qty, 
    sa.non_sellable_content_abzak_qty, 
    sa.non_sellable_showroom_6th_floor_qty, 
    sa.non_sellable_qatar_exhibition_qty, 
    sa.non_sellable_old_return_solv_qty, 
    sa.non_sellable_mgmt_qty, 
    sa.non_sellable_kwpurch_qty, 
    sa.non_sellable_showroom_m1_floor_qty, 
    sa.non_sellable_abrar_qty, 
    sa.non_sellable_marketing_gift_qty, 
    sa.non_sellable_new_showroom_qty, 
    sa.non_sellable_dubai_samp_qty, 
    sa.non_sellable_tv_mashour_qty, 
    sa.non_sellable_other_offline_qty, 
    sa.non_sellable_designer_hamdan_qty, 
    sa.non_sellable_sample_qty, 
    sa.non_sellable_other_qty, 
    sa.toal_nav_non_sellable, 
    sa.full_pending_open_po_qty, 
    sa.partially_pending_open_po_qty,
    sa.partial_pending_open_po_total_qty,
    sa.partial_pending_open_po_received_qty,
    sa.soh, 
    sa.stock_refreshed_datetime,
    COALESCE(sale.net_sale_qty_today,0) as net_sale_qty_today, COALESCE(sale.net_sale_qty_yesterday,0) as net_sale_qty_yesterday, COALESCE(sale.net_sale_qty_7days,0) as net_sale_qty_7days, COALESCE(sale.net_sale_qty_14days,0) as net_sale_qty_14days, COALESCE(sale.net_sale_qty_mtd,0) as net_sale_qty_mtd, COALESCE(sale.net_sale_qty_m1,0) as net_sale_qty_m1, COALESCE(sale.net_sale_qty_m2,0) as net_sale_qty_m2, COALESCE(sale.net_sale_qty_m3,0) as net_sale_qty_m3, COALESCE(sale.net_sale_qty_m4,0) as net_sale_qty_m4, COALESCE(sale.net_sale_qty_m5,0) as net_sale_qty_m5, COALESCE(sale.net_sale_qty_m6,0) as net_sale_qty_m6, sale.sale_sync_time,
    CASE
        WHEN nsm.department IS NULL THEN 'Department Not Assigned'::character varying
        ELSE nsm.department
    END AS department, 
    nsm.category_manager,
    GETDATE() as report_time
FROM nav.nav_sku_master nsm
LEFT JOIN nav.stock_agg sa ON sa.sku::text = nsm.sku::text
LEFT JOIN 
(
    select 
        sku, 
        sku_name, 
        sku_gender, 
        parent_sku, 
        size, 
        color, 
        celebrity_exclusive, 
        boutiqaat_exclusive, 
        special_price, 
        first_online_date 
    from magento.sku_master 
    where (sku||sku_type) not in (select sku||'simple' as sku_type
        from magento.sku_master 
        where sku_type <> 'bundle' 
        group by sku 
        having count(distinct parent_sku)>1) 
        and sku_type <> 'bundle'
) as msm ON msm.sku::text = nsm.sku::text 
LEFT JOIN aoi.consolidated_sku_stock crs ON crs.sku::text = nsm.sku::text
LEFT JOIN (select sku, 
          SUM(CASE WHEN order_date = today THEN net_sale_qty ELSE 0 END) net_sale_qty_today,
          SUM(CASE WHEN order_date = yesterday THEN net_sale_qty ELSE 0 END) net_sale_qty_yesterday,
          SUM(CASE WHEN order_date between (today - INTERVAL '7 day') AND yesterday THEN net_sale_qty ELSE 0 END) net_sale_qty_7days,
          SUM(CASE WHEN order_date between (today - INTERVAL '14 day') AND yesterday THEN net_sale_qty ELSE 0 END) net_sale_qty_14days,
          SUM(CASE WHEN order_date between mtd_first_day AND yesterday THEN net_sale_qty ELSE 0 END) net_sale_qty_mtd,
          SUM(CASE WHEN order_date between m1_first_day AND m1_last_day THEN net_sale_qty ELSE 0 END) net_sale_qty_m1,
          SUM(CASE WHEN order_date between m2_first_day AND m2_last_day THEN net_sale_qty ELSE 0 END) net_sale_qty_m2,
          SUM(CASE WHEN order_date between m3_first_day AND m3_last_day THEN net_sale_qty ELSE 0 END) net_sale_qty_m3,
          SUM(CASE WHEN order_date between m4_first_day AND m4_last_day THEN net_sale_qty ELSE 0 END) net_sale_qty_m4,
          SUM(CASE WHEN order_date between m5_first_day AND m5_last_day THEN net_sale_qty ELSE 0 END) net_sale_qty_m5,
          SUM(CASE WHEN order_date between m6_first_day AND m6_last_day THEN net_sale_qty ELSE 0 END) net_sale_qty_m6, MAX(sale_sync_at) sale_sync_time
          FROM 
          (select sku, order_date, sum(quantity) net_sale_qty, max(order_at) sale_sync_at from aoi.order_details where order_date >= DATE(LAST_DAY(CURRENT_DATE - INTERVAL '7 month') + INTERVAL '1 day') AND order_category<>'CELEBRITY' AND lower(order_status) not like '%cancel%' AND lower(order_status) not like '%ret%' group by 1,2) sku_net_sale,
          (select CURRENT_DATE as today,  DATE(CURRENT_DATE - INTERVAL '1 day') as yesterday,
                  DATE(LAST_DAY(CURRENT_DATE - INTERVAL '1 month') + INTERVAL '1 day') mtd_first_day,
                  LAST_DAY(CURRENT_DATE) mtd_last_day,
                  DATE(LAST_DAY(CURRENT_DATE - INTERVAL '2 month') + INTERVAL '1 day') m1_first_day,
                  LAST_DAY(CURRENT_DATE - INTERVAL '1 month') m1_last_day,
                  DATE(LAST_DAY(CURRENT_DATE - INTERVAL '3 month') + INTERVAL '1 day') m2_first_day,
                  LAST_DAY(CURRENT_DATE - INTERVAL '2 month') m2_last_day,
                  DATE(LAST_DAY(CURRENT_DATE - INTERVAL '4 month') + INTERVAL '1 day') m3_first_day,
                  LAST_DAY(CURRENT_DATE - INTERVAL '3 month') m3_last_day,
                  DATE(LAST_DAY(CURRENT_DATE - INTERVAL '5 month') + INTERVAL '1 day') m4_first_day,
                  LAST_DAY(CURRENT_DATE - INTERVAL '4 month') m4_last_day,
                  DATE(LAST_DAY(CURRENT_DATE - INTERVAL '7 month') + INTERVAL '1 day') m5_first_day,
                  LAST_DAY(CURRENT_DATE - INTERVAL '5 month') m5_last_day,
                  DATE(LAST_DAY(CURRENT_DATE - INTERVAL '7 month') + INTERVAL '1 day') m6_first_day,
                  LAST_DAY(CURRENT_DATE - INTERVAL '6 month') m6_last_day) sale_window
          group by sku) sale ON sale.sku::text = nsm.sku::text);
COMMIT;

