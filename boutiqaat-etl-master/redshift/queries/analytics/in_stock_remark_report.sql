BEGIN;
DROP TABLE IF EXISTS analytics.in_stock_remark_report;

SELECT * INTO analytics.in_stock_remark_report
FROM
(
SELECT
    t1.sku, 
    COALESCE(pc.parent_sku, t1.sku) parent_sku, 
    Replace(Replace(t1.description,'\t',''), '\n', '') AS description,
    t3.sku_type,
    t1.brand,
    t1.category1,
    t1.category2,
    t1.category3,
    t1.category4,
    t1.sync_from_nav_to_magento,
    t2.quantity_available_for_sale,
    t2.warehouse_reserved_qty,
    soh.open_po_total_qty, 
    soh.department, 
    soh.sold_qty_m2, 
    soh.sold_qty_lifetime,
    t2.is_quantity_sync_to_crs,
    t2.entry_no AS stock_entry_no,
    t3.magento_enabled_status,
    CASE 
        WHEN t3.magento_enabled_status = 'Enabled' AND t2.quantity_available_for_sale > 0 then 'Enabled_sku'
        WHEN t3.remark = 'NO Remarks' AND  t2.quantity_available_for_sale = 0 THEN 'SOH=0 & live'
        WHEN t2.quantity_available_for_sale = 0 THEN 'SOH=0'
        WHEN t3.remark = 'No Remarks' or t3.remark is NULL THEN '000-Content-Pending'
        WHEN t4.sku IS NOT NULL AND  t3.remark = 'NO remark' THEN 'Live' 
        ELSE t3.remark 
    END AS remark,
    CASE 
        WHEN t4.sku IS NOT NULL THEN 'Yes' 
        ELSE 'No' 
    END AS magento_force_sold_out_status
FROM
(
    SELECT
        No AS sku,
        description,
        brand,
        item_category_code AS category1,
        product_group_code AS category2,
        third_category AS category3,
        fourth_category AS category4,
        CASE
            WHEN sync_id IS NULL THEN 'No'
            ELSE 'Yes'
        END AS sync_from_nav_to_magento
    FROM nav.item
    GROUP BY 1,2,3,4,5,6,7,8
) t1
LEFT JOIN
(
    SELECT
        a.item_no ,
        a.entry_no,
        a.qty_in_stock AS quantity_available_for_sale,
        a.reserved_quantity AS warehouse_reserved_qty,
        CASE
            WHEN a.stock_sync_newstack = 1 THEN 'Yes'
            ELSE 'No'
        END AS is_quantity_sync_to_crs
    FROM nav.stock_details a
WHERE (a.item_no, a.entry_no) IN
    ( SELECT b.item_no, max(b.entry_no) FROM nav.stock_details b WHERE b.stock_sync_newstack = 1 GROUP BY b.item_no)
) t2 ON t1.sku = t2.item_no
LEFT JOIN
(
    select 
        sku, 
        sku_type, 
        listagg(distinct CASE WHEN attribute_value=1 THEN 'Enabled' WHEN attribute_value IS NOT NULL AND attribute_value<>1 THEN 'Disabled' END) AS magento_enabled_status, 
        listagg(distinct remark) AS remark 
    from
    (
        select * from 
        (
            SELECT 
                cpe.sku,cpe.type_id as sku_type, 
                cpei.attribute_id, 
                CASE 
                    WHEN cpei.attribute_id=96 THEN cpei.value 
                    ELSE NULL 
                END attribute_value, 
                eaov.value as remark, 
                row_number() OVER (PARTITION BY sku,cpei.attribute_id ORDER BY cpei.store_id ASC, eaov.store_id ASC) AS row_number
            FROM magento.catalog_product_entity cpe
            LEFT JOIN magento.catalog_product_entity_int cpei ON cpe.row_id = cpei.row_id
            LEFT JOIN magento.eav_attribute_option_value eaov ON eaov.option_id = cpei.value AND cpei.attribute_id=522
            WHERE attribute_id in (96,522) 
        ) where row_number=1
    ) magento_disabled_status_with_remark 
    GROUP BY 1,2
) t3 on t3.sku=t1.sku
LEFT JOIN sandbox.crs_sold_out_sku t4 on t4.sku = t1.sku
left join ofs.aoi_soh_report soh on soh.sku=t1.sku
LEFT JOIN analytics.parent_child_sku_mapping pc on pc.child_sku=t1.sku
);

COMMIT;