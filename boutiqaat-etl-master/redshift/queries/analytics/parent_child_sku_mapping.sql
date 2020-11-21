BEGIN;
DELETE FROM analytics.parent_child_sku_mapping WHERE 1=1;

INSERT INTO analytics.parent_child_sku_mapping
SELECT 
    A.parent_sku, 
    A.child_id, 
    B.sku child_sku,
    nav.brand brand,
    B.name sku_name, 
    B.description, 
    nav.item_category_code category1, 
    nav.product_group_code category2, 
    nav.third_category category3, 
    nav.fourth_category category4 
from (
    select 
        cpf1.row_id parent_row_id,
        cpf1.sku parent_sku,
        cpe.child_id,
        cpf1.entity_id
    from magento.catalog_product_entity cpf1 
    left join magento.catalog_product_relation cpe on cpe.parent_id = cpf1.row_id 
    where cpf1.type_id in ('configurable')
) A 
left join magento.catalog_product_flat_1 B on B.row_id = A.child_id 
left join nav.item nav on B.sku = nav.no 
group by 1,2,3,4,5,6,7,8,9,10;
COMMIT;