select 
    cp.row_id,
    cp.sku,
    parent_child.parent_sku,
    color.color,
	brand.brand_id,
	brand.brand_name,
    price.price as price,
	category.category_id,
    category.L3 as category1,
    category.L4 as category2,
    category.L5 as category3, 
    category.L6 as category4, 
    category.L7 as category5,
    concat('https://v2cdn.boutiqaat.com/media/catalog/product',cpv.value) AS image
from boutiqaat_v2.catalog_product_entity cp 
left join 
(
    select 
        cpe_child.sku as child_sku, 
        cpe_child.row_id as child_row_id, 
        cpe_parent.sku as parent_sku 
    from boutiqaat_v2.catalog_product_relation cpr
    left join boutiqaat_v2.catalog_product_entity cpe_child on cpe_child.row_id = cpr.child_id 
    left join boutiqaat_v2.catalog_product_entity cpe_parent on cpe_parent.row_id = cpr.parent_id 
    where cpe_parent.type_id = 'configurable'
) parent_child on parent_child.child_row_id = cp.row_id 
left join 
(
 SELECT cpei.row_id, cpei.attribute_id, eaov.value AS color
	FROM boutiqaat_v2.catalog_product_entity_int AS cpei
	LEFT JOIN eav_attribute_option_value AS eaov ON eaov.option_id = cpei.value  AND eaov.store_id = cpei.store_id
	WHERE cpei.attribute_id = 92
) color on color.row_id = cp.row_id 
left join
(
	select 
		  distinct cpei.row_id, cpe.sku, 
		  cpei.value as brand_id, 
		  bm.brand_english_name as brand_name  
	from boutiqaat_v2.catalog_product_entity cpe 
	JOIN boutiqaat_v2.catalog_product_entity_int cpei ON cpe.row_id = cpei.row_id 
	LEFT JOIN  boutiqaat_v2.brand_management bm on cpei.value = bm.brand_option_id 
	WHERE cpei.attribute_id = 81
) brand on brand.row_id = cp.row_id 
left join 
(
    select 
        * 
    from boutiqaat_v2.catalog_product_entity_varchar 
    where attribute_id = 85
) cpv on cpv.row_id = cp.row_id 
left join 
(
    select 
        price.row_id, 
        COALESCE (sprice.value,price.value) as price 
    from 
    (
        select 
            distinct row_id,
            value 
        from boutiqaat_v2.catalog_product_entity_decimal 
        where attribute_id = 75 and value is not null and value !=0
    ) price
    left join 
    (
        select 
            distinct row_id,
            value 
        from boutiqaat_v2.catalog_product_entity_decimal 
        where attribute_id = 76 and value is not null and value !=0
    ) sprice on sprice.row_id = price.row_id
) price on price.row_id = cp.row_id 
left join
(	
SELECT
t1.product_id,
t9.sku,
t9.type_id,
t2.row_id,
t1.category_id,
t3.value,
t2.level,
t2.path AS path,
CASE WHEN t2.level >= 2 THEN t4.value ELSE ''
END AS L3,
CASE WHEN t2.level >= 3 THEN t5.value ELSE ''
END AS L4,
CASE WHEN t2.level >= 4 THEN t6.value ELSE ''
END AS L5,
CASE WHEN t2.level >= 5 THEN t7.value ELSE ''
END AS L6,
CASE WHEN t2.level >= 6 THEN t8.value ELSE ''
END AS L7
FROM
catalog_category_product AS t1
JOIN catalog_category_entity AS t2 ON t1.category_id = t2.entity_id AND t2.level > 3
JOIN catalog_category_entity_varchar AS t3 ON t2.row_id = t3.row_id AND t3.attribute_id = 41 AND t3.store_id = 0
JOIN catalog_category_entity_varchar AS t4 ON t4.row_id = SUBSTRING_INDEX(
SUBSTRING_INDEX(t2.path, '/', 3),
'/',
-1
) AND t4.attribute_id = 41 AND t4.store_id = 0
JOIN catalog_category_entity_varchar AS t5 ON t5.row_id = SUBSTRING_INDEX(
SUBSTRING_INDEX(t2.path, '/', 4),
'/',
-1
) AND t5.attribute_id = 41 AND t5.store_id = 0
JOIN catalog_category_entity_varchar AS t6
ON
t6.row_id = SUBSTRING_INDEX(
SUBSTRING_INDEX(t2.path, '/', 5),
'/',
-1
) AND t6.attribute_id = 41 AND t6.store_id = 0
JOIN catalog_category_entity_varchar AS t7
ON
t7.row_id = SUBSTRING_INDEX(
SUBSTRING_INDEX(t2.path, '/', 6),
'/',
-1
) AND t7.attribute_id = 41 AND t7.store_id = 0
JOIN  catalog_category_entity_varchar AS t8
ON
t8.row_id = SUBSTRING_INDEX(
SUBSTRING_INDEX(t2.path, '/', 7),
'/',
-1
) AND t8.attribute_id = 41 AND t8.store_id = 0
JOIN catalog_product_entity AS t9 ON t9.entity_id = t1.product_id
WHERE
(IF(LOCATE(2741,t2.path)>0,1,0) OR
IF(LOCATE(4194,t2.path)>0,1,0)) AND
t2.level > 4
group by t1.product_id
ORDER BY
t1.product_id
) category 
on category.product_id = cp.row_id 
where \$CONDITIONS and cp.type_id not in ('configurable','bundle') group by cp.row_id

