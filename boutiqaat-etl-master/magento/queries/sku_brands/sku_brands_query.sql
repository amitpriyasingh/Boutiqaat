select DISTINCT 
	cpe.sku,
	cpei.brand_id, 
	bm.brand_english_name as brand_name  
from boutiqaat_v2.catalog_product_entity cpe 
JOIN (select row_id, value as brand_id, attribute_id from boutiqaat_v2.catalog_product_entity_int) cpei ON cpe.row_id = cpei.row_id 
JOIN  boutiqaat_v2.brand_management bm on cpei.brand_id = bm.brand_option_id 
WHERE \$CONDITIONS and cpei.attribute_id = 81

