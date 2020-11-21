BEGIN;
DROP TABLE IF EXISTS analytics.sku_live_status_report;
CREATE TABLE analytics.sku_live_status_report
(
    sku VARCHAR(40) ENCODE LZO
    ,description VARCHAR(400) ENCODE LZO
    ,brand VARCHAR(40) ENCODE LZO
    ,category1 VARCHAR(40) ENCODE LZO
    ,category2 VARCHAR(40) ENCODE LZO
    ,category3 VARCHAR(40) ENCODE LZO
    ,category4 VARCHAR(40) ENCODE LZO
    ,sync_from_nav_to_magento INTEGER ENCODE LZO
    ,quantity_available_for_sale INTEGER ENCODE LZO
    ,warehouse_reserved_qty INTEGER ENCODE LZO
    ,is_quantity_sync_to_crs VARCHAR(3) ENCODE LZO
    ,stock_entry_no INTEGER ENCODE LZO
    ,magento_enabled_status VARCHAR(8) ENCODE LZO
    ,magento_sold_out_status INTEGER ENCODE LZO
    ,synched_at_utc TIMESTAMP ENCODE LZO
)DISTSTYLE KEY DISTKEY (sku);

insert into analytics.sku_live_status_report
(
    sku, 
    description, 
    brand, 
    category1, 
    category2, 
    category3, 
    category4, 
    sync_from_nav_to_magento, 
    quantity_available_for_sale, 
    warehouse_reserved_qty, 
    is_quantity_sync_to_crs, 
    stock_entry_no, 
    magento_enabled_status, 
    magento_sold_out_status,
    synched_at_utc
)
SELECT 
    t1.sku, 
    Replace(Replace(t1.description,'\t','-'), '\n', '-') AS description, 
    t1.brand,
    t1.category1, 
    t1.category2, 
    t1.category3, 
    t1.category4,
    t1.sync_from_nav_to_magento, 
    t2.quantity_available_for_sale,
    t2.warehouse_reserved_qty, 
    t2.is_quantity_sync_to_crs,
    t2.entry_no AS stock_entry_no, 
    t4.magento_enabled_status,
    CASE WHEN t5.sku IS NOT NULL THEN 1 ELSE 0 END AS magento_sold_out_status,
    GETDATE() as synched_at_utc
FROM 
(
    SELECT 
        "no" AS sku, 
        description, 
        brand, 
        item_category_code AS category1,
        product_group_code AS category2, 
        third_category AS category3, 
        fourth_category AS category4,
        CASE WHEN sync_id IS NULL THEN 0 ELSE 1 END AS sync_from_nav_to_magento
    FROM nav.item
    GROUP BY 1,2,3,4,5,6,7,8
) t1
LEFT JOIN
(
    SELECT 
        a.item_no, 
        a.entry_no, 
        a.qty_in_stock AS quantity_available_for_sale, 
        a.reserved_quantity AS warehouse_reserved_qty,
        CASE WHEN a.stock_sync_newstack = 1 THEN 'YES' ELSE 'NO' END AS is_quantity_sync_to_crs
    FROM nav.stock_details a 
    WHERE (a.item_no, a.entry_no) IN (
            SELECT 
                b.item_no, 
                max(b.entry_no) 
            FROM nav.stock_details b 
            WHERE b.stock_sync_newstack = 1 GROUP BY b.item_no)
) t2 ON t1.sku = t2.item_no
LEFT JOIN 
(
    SELECT 
        t3.m_sku, 
        coalesce(t3.store0_magento_enabled_status, t3.store1_magento_enabled_status, t3.store3_magento_enabled_status) as magento_enabled_status
    FROM 
    (
        SELECT 
            m.sku as m_sku, 
            (CASE WHEN n.row_id IS NULL OR n.value IS NULL OR n.store_id!=0 THEN NULL
                WHEN n.value = 1 THEN 'enabled' 
                ELSE 'disabled' 
            END) AS store0_magento_enabled_status,
            (CASE WHEN n.row_id IS NULL OR n.value IS NULL OR n.store_id!=1 THEN NULL
                WHEN n.value = 1 THEN 'enabled'
                ELSE 'disabled' 
            END) AS store1_magento_enabled_status,
            (CASE WHEN n.row_id IS NULL OR n.value IS NULL OR n.store_id!=3 THEN NULL
                WHEN n.value = 1 THEN 'enabled' 
                ELSE 'disabled' 
            END) AS store3_magento_enabled_status
        FROM magento.catalog_product_entity m
        LEFT JOIN magento.catalog_product_entity_int n ON m.row_id = n.row_id
        WHERE n.attribute_id = 96 OR n.attribute_id IS NULL
        GROUP BY m.sku,n.row_id ,n.value,n.store_id
    ) t3
    group by m_sku, magento_enabled_status
) t4 on t4.m_sku=t1.sku
LEFT JOIN (select sku from aoi.consolidated_sku_stock where crs_force_soldout = '1' group by sku) t5 on t5.sku = t1.sku;
COMMIT;

