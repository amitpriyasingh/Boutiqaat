SELECT DISTINCT sku_cats.sku, 
                sku_cats.category_id, 
                cat_name.value AS category_name 
FROM   (SELECT cpe.sku, 
               COALESCE(ccp_child.category_id, ccp_parent.category_id) as category_id 
        FROM   boutiqaat_v2.catalog_product_entity cpe 
               LEFT JOIN boutiqaat_v2.catalog_category_product ccp_child 
                      ON cpe.row_id = ccp_child.product_id 
               LEFT JOIN boutiqaat_v2.catalog_product_relation cpr 
                      ON cpe.row_id = cpr.child_id 
               LEFT JOIN boutiqaat_v2.catalog_category_product ccp_parent 
                      ON cpr.parent_id = ccp_parent.product_id 
        GROUP  BY 1, 2
	   ) sku_cats 
       LEFT JOIN boutiqaat_v2.catalog_category_entity AS cce 
              ON sku_cats.category_id = cce.entity_id 
       LEFT JOIN boutiqaat_v2.catalog_category_entity_varchar AS cat_name 
              ON cce.row_id = cat_name.row_id 
                 AND cat_name.attribute_id = 41 AND cat_name.store_id = 0
WHERE \$CONDITIONS and sku_cats.category_id > 2				 


-- SELECT distinct 
    -- t4.sku,
    -- t1.category_id,
    -- t3.value category_name
-- FROM boutiqaat_v2.catalog_category_product AS t1
-- JOIN boutiqaat_v2.catalog_category_entity AS t2 ON t1.category_id = t2.entity_id
-- JOIN boutiqaat_v2.catalog_category_entity_varchar AS t3 ON t2.row_id = t3.row_id 
-- AND t3.attribute_id = 41 AND t3.store_id = 0
-- JOIN boutiqaat_v2.catalog_product_entity AS t4 ON t4.row_id=t1.product_id
-- WHERE \$CONDITIONS and t1.category_id > 2 


