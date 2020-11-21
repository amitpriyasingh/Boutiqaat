CREATE OR REPLACE VIEW analytics.product_catalog_report AS 
SELECT 
    magento.parent_sku, 
    MAX(nav.vendor_item_no::text) AS supplier_item_no, 
    MAX(nav.brand::text) AS brand, 
    MAX(magento.sku_gender::text) AS gender, 
    MAX(nav.department::text) AS department, 
    MAX(magento.color::text) AS color, 
    MAX(magento.image_url::text) AS image_url, 
    MAX(magento.category1::text) AS category1, 
    MAX(magento.category2::text) AS category2, 
    MAX(magento.category3::text) AS category3, 
    MAX(magento.price) AS price, 
    MAX(magento.special_price) AS special_price, 
    sum(aoi.sold_qtys) AS solt_qty, 
    sum(stock.total_sellable_qty) AS total_sellable_qty, 
    sum(stock.toal_nav_non_sellable) AS toal_nav_non_sellable, 
    sum(stock.soh) AS soh, 
    CASE
        WHEN COALESCE(sum(aoi.sold_qtys)::numeric::numeric(18,0) + sum(stock.soh), 0::numeric::numeric(18,0)) <> 0::numeric::numeric(18,0) THEN round(sum(aoi.sold_qtys)::numeric::numeric(18,0) / (sum(aoi.sold_qtys)::numeric::numeric(18,0) + sum(stock.soh)), 3)
        ELSE 0::numeric::numeric(18,0)
    END AS sell_through, 
    MAX(stock.stock_refreshed_datetime) AS stock_refreshed_datetime
FROM nav.nav_sku_master nav
LEFT JOIN nav.stock_agg stock ON stock.sku::text = nav.sku::text
LEFT JOIN magento.sku_master magento ON magento.sku::text = nav.sku::text
LEFT JOIN ( SELECT ofs_sku_sales.sku, sum(ofs_sku_sales.sold_qtys) AS sold_qtys
FROM aoi.ofs_sku_sales
GROUP BY ofs_sku_sales.sku) aoi ON aoi.sku::text = nav.sku::text
WHERE (nav.department::text = 'FASHIONSPORTS'::text OR nav.department::text = 'SUPPLEMENTSFITNESS'::text OR nav.department::text = 'TRADITIONALWEAR'::text) AND magento.sku_type::text = 'configurable'::text
GROUP BY magento.parent_sku
WITH NO SCHEMA BINDING;