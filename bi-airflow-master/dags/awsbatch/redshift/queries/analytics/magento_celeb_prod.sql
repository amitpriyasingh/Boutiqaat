BEGIN;
DROP TABLE IF EXISTS analytics.magento_celeb_prod;
SELECT * INTO analytics.magento_celeb_prod
FROM
(
    SELECT 
        ccp.created_at as created_at,
        cp.celebrity_id as celebrity_id ,
        cp.ad_number as label,
        cm.celebrity_name  as celebrity_name,
        cp.product_entity_id as product_entity_id,
        ccp.sku as  sku,
        eaov.value as brand,
        cpev.value as sku_name,
        cm.celebrity_name || '--' || cp.celebrity_id as celeb_name_code
    FROM aoi.bi_celebrity_master AS cm
    LEFT JOIN magento.celebrity_product AS cp ON cm.celebrity_id = cp.celebrity_id
    LEFT JOIN magento.catalog_product_entity AS ccp ON ccp.entity_id = cp.product_entity_id
    LEFT JOIN magento.catalog_product_entity_varchar cpev ON cpev.row_id = ccp.row_id AND cpev.attribute_id = 71 AND cpev.store_id = 0
    LEFT JOIN magento.catalog_product_entity_int cpeim ON cpeim.row_id = ccp.row_id AND cpeim.attribute_id = 81 AND cpeim.store_id = 0
    LEFT JOIN magento.eav_attribute_option_value eaov ON eaov.option_id = cpeim.value AND eaov.store_id = 0
    WHERE ccp.sku IS NOT NULL ORDER BY ccp.sku
);
COMMIT;
