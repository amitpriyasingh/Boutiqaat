SET @max_child_id=0, @max_parent_id=0;
-- select max(child_id) INTO @max_child_id, max(parent_id) INTO @max_parent_id from dwh.sku_master_base;
-- 1 sec

drop table if exists dwh.sku_temp;

create temporary table dwh.sku_temp 
(PRIMARY KEY pair_idx (child_id,parent_id))
select cpe_s.row_id as child_id, min(cpe_s.row_id) as parent_id, min(cpe_s.sku) as sku, min(cpe_s.sku) as parent_sku, MIN(cpe_s.type_id) as type, 
DATE(min(cpe_s.created_at)) as created_date from (select row_id, sku, created_at, type_id from boutiqaat_v2.catalog_product_entity 
where type_id='simple' and sku is not null and row_id > @max_child_id) cpe_s 
left join boutiqaat_v2.catalog_product_relation cpr_s on cpe_s.row_id = cpr_s.child_id 
left join boutiqaat_v2.catalog_product_entity cpe_p on cpe_p.row_id = cpr_s.parent_id
group by 1
having count(cpr_s.child_id) = 0 or sum(case when cpe_p.type_id='configurable' then 1 else 0 end)=0
union
select child_id, parent_id, cpe_s.sku, cpe_p.sku parent_sku, cpe_p.type_id as type, created_date from
	(select cpe_s.row_id as child_id, cpr_s.parent_id, min(cpe_s.sku) as sku, DATE(min(cpe_s.created_at)) as created_date 
		from (select row_id, sku, created_at from boutiqaat_v2.catalog_product_entity where sku is not null and type_id='simple' and row_id > @max_child_id) cpe_s 
		left join boutiqaat_v2.catalog_product_relation cpr_s on cpe_s.row_id = cpr_s.child_id where cpr_s.parent_id is not null group by 1,2) as cpe_s 
	join boutiqaat_v2.catalog_product_entity cpe_p on cpe_p.row_id = cpe_s.parent_id where cpe_p.type_id='configurable'
union
select child_id, parent_id, cpe_bs.sku, cpe_b.sku parent_sku, cpe_b.type_id as type, created_date from
	(select cpe_s.row_id as child_id, cpbs.parent_product_id as parent_id, min(cpe_s.sku) as sku, DATE(min(cpe_s.created_at)) as created_date 
		from (select row_id, sku, created_at from boutiqaat_v2.catalog_product_entity where sku is not null and type_id='simple' and row_id > @max_child_id) cpe_s 
		left join boutiqaat_v2.catalog_product_bundle_selection cpbs on cpe_s.row_id = cpbs.product_id where cpbs.parent_product_id is not null group by 1,2) as cpe_bs 
	join boutiqaat_v2.catalog_product_entity cpe_b on cpe_b.row_id = cpe_bs.parent_id where cpe_b.type_id='bundle';

-- 4 sec

drop table if exists dwh.sku_master_base;

create table dwh.sku_master_base (PRIMARY KEY pair_idx (child_id,parent_id), index child_id (child_id), index parent_id (parent_id)) select * from dwh.sku_temp;

-- override single child bundle as parent to be simple SKU when the parent is disabled & child is enabled
UPDATE IGNORE dwh.sku_master_base as master
JOIN (SELECT sm.parent_id, sm.parent_sku, sm.child_id, sm.sku, sm.type  
FROM dwh.sku_temp sm
LEFT JOIN boutiqaat_v2.catalog_product_entity_int as cpei_p ON sm.parent_id = cpei_p.row_id AND cpei_p.attribute_id=96 AND cpei_p.store_id = 0
LEFT JOIN boutiqaat_v2.catalog_product_entity_int as cpei_c ON sm.child_id = cpei_c.row_id AND cpei_c.attribute_id=96 AND cpei_c.store_id = 0
WHERE type = 'bundle' and cpei_p.value = 2 and cpei_c.value = 1
group by sm.parent_id having count(sm.child_id)=1) liberated_children ON master.child_id = liberated_children.child_id
SET master.type='simple', master.parent_id = master.child_id, master.parent_sku = master.sku
WHERE 1=1;

drop table if exists dwh.parent_props_name_temp;

create temporary table dwh.parent_props_name_temp 
(PRIMARY KEY parent_id (parent_id), INDEX parent_sku (parent_sku(20)) )
select parent_id, parent_sku, group_concat(distinct REPLACE(REPLACE(REPLACE(REPLACE(TRIM(name.value), '\r', ''),'\n',''),'\t',''),'\"', '')) as sku_name
FROM (select distinct parent_id, parent_sku from dwh.sku_master_base where parent_id > @max_parent_id 
-- or sku_name is null
) as parent
LEFT JOIN boutiqaat_v2.catalog_product_entity_varchar as name ON parent.parent_id = name.row_id and attribute_id = 71 AND store_id = 0
group by 1;
-- 3 sec



drop table if exists dwh.parent_props_gender_temp;

create temporary table dwh.parent_props_gender_temp (PRIMARY KEY parent_id (parent_id)) select parent_id, 
group_concat(distinct case when gender.value like '%4194,2741%' or gender.value like '%2741,4194%' then 'unisex' when gender.value like '%4194%' THEN 'women' when gender.value like '%2741%' THEN 'men' else gender.value end) as gender
FROM (select distinct parent_id from dwh.sku_master_base where parent_id > @max_parent_id 
-- or gender is null
) as parent_sku
LEFT JOIN boutiqaat_v2.catalog_product_entity_text gender ON parent_sku.parent_id = gender.row_id and attribute_id = 542 AND store_id = 0
group by 1;
-- 1.5 sec

drop table if exists dwh.parent_props_brand_temp;

create temporary table dwh.parent_props_brand_temp (PRIMARY KEY parent_id (parent_id)) select prod.*, brand_english_name as brand from (select parent_id, 
group_concat(distinct case when cpei.attribute_id=265 then coalesce(cpei.value,0) end) as boutiqaat_exclusive, 
group_concat(distinct case when cpei.attribute_id=96 then cpei.value end) as enable_status,
group_concat(distinct case when cpei.attribute_id=81 then cpei.value end) as brand_id
FROM (select distinct parent_id from dwh.sku_master_base where parent_id > @max_parent_id 
-- or boutiqaat_exclusive is null or brand_id is null
) as parent_sku
LEFT JOIN boutiqaat_v2.catalog_product_entity_int as cpei ON parent_sku.parent_id = cpei.row_id AND cpei.attribute_id in (81,96,265) AND cpei.store_id = 0 group by 1) as prod
LEFT JOIN boutiqaat_v2.brand_management bm on prod.brand_id = bm.brand_option_id;
-- 6 sec


drop table if exists dwh.celeb_exclusive_temp;

create temporary table dwh.celeb_exclusive_temp (PRIMARY KEY product_entity_id (product_entity_id)) select product_entity_id, 
SUM(CASE WHEN product_status='Online' then CASE WHEN COALESCE(is_exclusive,'No') = 'Yes' THEN 1 ELSE 0 END ELSE 0 END) as celebrity_exclusive
FROM boutiqaat_v2.celebrity_product -- where DATE(updated_at) >= DATE(now() - interval 7 day)
group by 1;
-- 5 sec

drop table if exists dwh.parent_props_category_temp;

SET @row_number = 0, @p_id= 0;

create temporary table dwh.parent_props_category_temp (PRIMARY KEY parent_id (parent_id)) 
select parent_id, 
CASE WHEN level >= 3 THEN cat1_name ELSE ''
END AS category1,
CASE WHEN level >= 4 THEN cat2_name ELSE ''
END AS category2,
CASE WHEN level >= 5 THEN cat3_name ELSE ''
END AS category3,
CASE WHEN level >= 6 THEN cat4_name ELSE ''
END AS category4,
CASE WHEN level >= 3 THEN cat1_id ELSE ''
END AS category1_id,
CASE WHEN level >= 4 THEN cat2_id ELSE ''
END AS category2_id,
CASE WHEN level >= 5 THEN cat3_id ELSE ''
END AS category3_id,
CASE WHEN level >= 6 THEN cat4_id ELSE ''
END AS category4_id,
CONCAT( '--', GROUP_CONCAT(DISTINCT CASE WHEN level >= 3 THEN cat1_name ELSE '' END ORDER BY cat1_id ASC SEPARATOR '--'), '--') AS category1_list,
CONCAT( '--', GROUP_CONCAT(DISTINCT CASE WHEN level >= 4 THEN cat2_name ELSE ''
END ORDER BY cat2_id ASC SEPARATOR '--'), '--')  AS category2_list,
CONCAT( '--', GROUP_CONCAT(DISTINCT CASE WHEN level >= 5 THEN cat3_name ELSE ''
END ORDER BY cat3_id ASC SEPARATOR '--'), '--')  AS category3_list,
CONCAT( '--', GROUP_CONCAT(DISTINCT CASE WHEN level >= 6 THEN cat4_name ELSE ''
END ORDER BY cat4_id ASC SEPARATOR '--'), '--')  AS category4_list,
CONCAT( '--', GROUP_CONCAT(DISTINCT CASE WHEN level >= 3 THEN cat1_id ELSE ''
END ORDER BY cat1_id ASC SEPARATOR '--'), '--')  AS category1_list_id,
CONCAT( '--', GROUP_CONCAT(DISTINCT CASE WHEN level >= 4 THEN cat2_id ELSE ''
END ORDER BY cat2_id ASC SEPARATOR '--'), '--')  AS category2_list_id,
CONCAT( '--', GROUP_CONCAT(DISTINCT CASE WHEN level >= 5 THEN cat3_id ELSE ''
END ORDER BY cat3_id ASC SEPARATOR '--'), '--')  AS category3_list_id,
CONCAT( '--', GROUP_CONCAT(DISTINCT CASE WHEN level >= 6 THEN cat4_id ELSE ''
END ORDER BY cat4_id ASC SEPARATOR '--'), '--')  AS category4_list_id
FROM 
(select @row_number:=CASE WHEN @p_id=parent_id THEN @row_number + 1 ELSE 1 END AS row_num, @p_id:=parent_id, ordered_cats.*
from (select parent_sku.parent_id parent_id, cce.level, cce.`position`, cce.path, cat1_name.row_id cat1_id, cat2_name.row_id cat2_id,  cat3_name.row_id cat3_id, cat4_name.row_id cat4_id, cat1_name.value cat1_name, cat2_name.value cat2_name, cat3_name.value cat3_name, cat4_name.value cat4_name
FROM (select distinct parent_id from dwh.sku_master_base where parent_id > @max_parent_id
-- or category1 is null or category2 is null
) as parent_sku
JOIN boutiqaat_v2.catalog_category_product ccp ON parent_sku.parent_id = ccp.product_id 
JOIN boutiqaat_v2.catalog_category_entity AS cce ON ccp.category_id = cce.entity_id and level >= 3
JOIN boutiqaat_v2.catalog_category_entity_varchar AS cat1_name ON SUBSTRING_INDEX(SUBSTRING_INDEX(cce.path, '/', 4),'/',-1) = cat1_name.row_id AND cat1_name.attribute_id = 41 AND cat1_name.store_id = 0
JOIN boutiqaat_v2.catalog_category_entity_varchar AS cat2_name ON SUBSTRING_INDEX(SUBSTRING_INDEX(cce.path, '/', 5),'/',-1) = cat2_name.row_id AND cat2_name.attribute_id = 41 AND cat2_name.store_id = 0
JOIN boutiqaat_v2.catalog_category_entity_varchar AS cat3_name ON SUBSTRING_INDEX(SUBSTRING_INDEX(cce.path, '/', 6),'/',-1) = cat3_name.row_id AND cat3_name.attribute_id = 41 AND cat3_name.store_id = 0
JOIN boutiqaat_v2.catalog_category_entity_varchar AS cat4_name ON SUBSTRING_INDEX(SUBSTRING_INDEX(cce.path, '/', 7),'/',-1) = cat4_name.row_id AND cat4_name.attribute_id = 41 AND cat4_name.store_id = 0
WHERE
(IF(LOCATE(2741,cce.path)>0,1,0) OR
IF(LOCATE(4194,cce.path)>0,1,0)) AND cce.level >= 3
order by parent_id, cce.level desc, cce.position desc, cce.path desc
) ordered_cats) ranked_cats
where row_num=1
group by 1;
-- 9 sec

update dwh.parent_props_category_temp cat JOIN (select parent_id,
CONCAT( '--', GROUP_CONCAT(DISTINCT CASE WHEN level >= 3 THEN cat1_name ELSE '' END ORDER BY cat1_id ASC SEPARATOR '--'), '--') AS category1_list,
CONCAT( '--', GROUP_CONCAT(DISTINCT CASE WHEN level >= 4 THEN cat2_name ELSE ''
END ORDER BY cat2_id ASC SEPARATOR '--'), '--')  AS category2_list,
CONCAT( '--', GROUP_CONCAT(DISTINCT CASE WHEN level >= 5 THEN cat3_name ELSE ''
END ORDER BY cat3_id ASC SEPARATOR '--'), '--')  AS category3_list,
CONCAT( '--', GROUP_CONCAT(DISTINCT CASE WHEN level >= 6 THEN cat4_name ELSE ''
END ORDER BY cat4_id ASC SEPARATOR '--'), '--')  AS category4_list,
CONCAT( '--', GROUP_CONCAT(DISTINCT CASE WHEN level >= 3 THEN cat1_id ELSE ''
END ORDER BY cat1_id ASC SEPARATOR '--'), '--')  AS category1_list_id,
CONCAT( '--', GROUP_CONCAT(DISTINCT CASE WHEN level >= 4 THEN cat2_id ELSE ''
END ORDER BY cat2_id ASC SEPARATOR '--'), '--')  AS category2_list_id,
CONCAT( '--', GROUP_CONCAT(DISTINCT CASE WHEN level >= 5 THEN cat3_id ELSE ''
END ORDER BY cat3_id ASC SEPARATOR '--'), '--')  AS category3_list_id,
CONCAT( '--', GROUP_CONCAT(DISTINCT CASE WHEN level >= 6 THEN cat4_id ELSE ''
END ORDER BY cat4_id ASC SEPARATOR '--'), '--')  AS category4_list_id
FROM
(select parent_sku.parent_id parent_id, cce.level, cce.`position`, cce.path, cat1_name.row_id cat1_id, cat2_name.row_id cat2_id,  cat3_name.row_id cat3_id, cat4_name.row_id cat4_id, cat1_name.value cat1_name, cat2_name.value cat2_name, cat3_name.value cat3_name, cat4_name.value cat4_name
FROM (select distinct parent_id from dwh.sku_master_base where parent_id > @max_parent_id
-- or category1 is null or category2 is null
) as parent_sku
JOIN boutiqaat_v2.catalog_category_product ccp ON parent_sku.parent_id = ccp.product_id 
JOIN boutiqaat_v2.catalog_category_entity AS cce ON ccp.category_id = cce.entity_id and level >= 3
JOIN boutiqaat_v2.catalog_category_entity_varchar AS cat1_name ON SUBSTRING_INDEX(SUBSTRING_INDEX(cce.path, '/', 4),'/',-1) = cat1_name.row_id AND cat1_name.attribute_id = 41 AND cat1_name.store_id = 0
JOIN boutiqaat_v2.catalog_category_entity_varchar AS cat2_name ON SUBSTRING_INDEX(SUBSTRING_INDEX(cce.path, '/', 5),'/',-1) = cat2_name.row_id AND cat2_name.attribute_id = 41 AND cat2_name.store_id = 0
JOIN boutiqaat_v2.catalog_category_entity_varchar AS cat3_name ON SUBSTRING_INDEX(SUBSTRING_INDEX(cce.path, '/', 6),'/',-1) = cat3_name.row_id AND cat3_name.attribute_id = 41 AND cat3_name.store_id = 0
JOIN boutiqaat_v2.catalog_category_entity_varchar AS cat4_name ON SUBSTRING_INDEX(SUBSTRING_INDEX(cce.path, '/', 7),'/',-1) = cat4_name.row_id AND cat4_name.attribute_id = 41 AND cat4_name.store_id = 0
WHERE
(IF(LOCATE(2741,cce.path)>0,1,0) OR
IF(LOCATE(4194,cce.path)>0,1,0)) AND cce.level >= 3) all_products
group by 1) cat_list ON cat_list.parent_id = cat.parent_id
SET cat.category1_list=cat_list.category1_list, cat.category2_list=cat_list.category2_list, cat.category3_list=cat_list.category3_list, cat.category4_list=cat_list.category4_list,
cat.category1_list_id=cat_list.category1_list_id, cat.category2_list_id=cat_list.category2_list_id, cat.category3_list_id=cat_list.category3_list_id, cat.category4_list_id=cat_list.category4_list_id
WHERE 1=1;
-- 12 sec

drop table if exists dwh.child_props_in_parent_temp;

create temporary table dwh.child_props_in_parent_temp (PRIMARY KEY parent_id (parent_id)) select parent_sku.parent_id, 
group_concat(distinct CASE WHEN color_attr.attribute_id IN (92) THEN eaov.value ELSE NULL END) as color, 
group_concat(distinct concat('https://v2cdn.boutiqaat.com/media/catalog/product/',image.value)) image_url
FROM (select distinct parent_id from dwh.sku_master_base where parent_id > @max_parent_id) as parent_sku
LEFT JOIN boutiqaat_v2.catalog_product_entity_int as color_attr ON parent_sku.parent_id = color_attr.row_id AND color_attr.attribute_id in (92) AND color_attr.store_id = 0
LEFT JOIN boutiqaat_v2.eav_attribute_option_value eaov on eaov.option_id=color_attr.value
LEFT JOIN boutiqaat_v2.catalog_product_entity_varchar image ON parent_sku.parent_id = image.row_id and image.attribute_id=85 and image.store_id=0
group by 1;
-- 3 sec

drop table if exists dwh.sku_status_history;

create temporary table dwh.sku_status_history (PRIMARY KEY sku (sku(20))) 
select sku_boundry_entry.sku, CASE WHEN first_entry.status = 1 THEN DATE(first_entry.created_date) ELSE null END first_online_date, CASE WHEN last_entry.status = 2 THEN 1 ELSE 0 END is_disabled_now from
(select sku, min(id) min_id, max(id) max_id FROM boutiqaat_v2.sku_status_history group by 1) sku_boundry_entry
join boutiqaat_v2.sku_status_history first_entry on first_entry.id=sku_boundry_entry.min_id
join boutiqaat_v2.sku_status_history last_entry on last_entry.id=sku_boundry_entry.max_id;
-- 1.5 sec

ALTER TABLE dwh.sku_status_history CONVERT TO CHARACTER SET utf8;

drop table if exists dwh.parent_props_temp;

create temporary table dwh.parent_props_temp (PRIMARY KEY parent_id (parent_id)) select parent_sku.parent_id, sku_name, gender, brand_id, brand, boutiqaat_exclusive, CASE WHEN celebrity_exclusive > 0 THEN 1 ELSE 0 END as celebrity_exclusive, category1_id, category2_id, category3_id, category4_id, category1, category2, category3, category4, 
category1_list, category2_list, category3_list, category4_list, category1_list_id, category2_list_id, category3_list_id, category4_list_id, color, image_url, first_online_date, is_disabled_now, enable_status as current_enable_status 
FROM 
-- (select distinct parent_id from dwh.sku_master_base where parent_id > @max_parent_id) as parent_sku LEFT JOIN 
dwh.parent_props_name_temp as parent_sku -- name_part on parent_sku.parent_id = name_part.parent_id
LEFT JOIN dwh.parent_props_gender_temp as gender_part on parent_sku.parent_id = gender_part.parent_id
LEFT JOIN dwh.parent_props_brand_temp as brand_part on parent_sku.parent_id = brand_part.parent_id
LEFT JOIN dwh.celeb_exclusive_temp as clx_part on parent_sku.parent_id = clx_part.product_entity_id
LEFT JOIN dwh.parent_props_category_temp as cat_part on parent_sku.parent_id = cat_part.parent_id
LEFT JOIN dwh.child_props_in_parent_temp as child_prop_part on parent_sku.parent_id = child_prop_part.parent_id
LEFT JOIN dwh.sku_status_history as status_part on parent_sku.parent_sku = status_part.sku;
-- 4 sec


drop table if exists dwh.child_props_temp;

create temporary table dwh.child_props_temp (PRIMARY KEY child_id (child_id)) 
select child_id, 
group_concat(distinct CASE WHEN props.attribute_id IN (539,293,538,519,509,512,541) THEN eaov.value ELSE NULL END) as size, 
group_concat(distinct CASE WHEN props.attribute_id IN (92) THEN eaov.value ELSE NULL END) as color, 
MAX(CASE WHEN price.attribute_id=75 THEN price.value END) price, MAX(CASE WHEN price.attribute_id=76 THEN price.value END) special_price,
group_concat(distinct concat('https://v2cdn.boutiqaat.com/media/catalog/product/',image.value)) image_url,
first_online_date, is_disabled_now, 
MAX(CASE WHEN props.attribute_id=96 THEN props.value ELSE NULL END) as current_enable_status
FROM (select distinct child_id, sku from dwh.sku_master_base where child_id > @max_child_id) as child_sku
LEFT JOIN boutiqaat_v2.catalog_product_entity_int as props ON child_sku.child_id = props.row_id AND props.attribute_id in (539,293,538,519,509,512,541,92,96) AND props.store_id = 0
LEFT JOIN boutiqaat_v2.eav_attribute_option_value eaov on eaov.option_id=props.value
LEFT JOIN boutiqaat_v2.catalog_product_entity_varchar image ON child_sku.child_id = image.row_id and image.attribute_id=85 and image.store_id=0
LEFT JOIN boutiqaat_v2.catalog_product_entity_decimal AS price ON child_sku.child_id = price.row_id AND price.attribute_id in (75,76) AND price.store_id = 0
LEFT JOIN dwh.sku_status_history as status_part on child_sku.sku = status_part.sku
group by 1;
-- 28 sec

drop table if exists dwh.sku_master;

create table dwh.sku_master(PRIMARY KEY (parent_id, child_id), index parent_sku (parent_sku), index parent_id (parent_id), index child_id (child_id))
select sku, parent_sku, type as sku_type, created_date, coalesce(p.sku_name,parent_sku) as sku_name, p.gender sku_gender, p.brand_id, p.brand, COALESCE(p.boutiqaat_exclusive,0) as boutiqaat_exclusive, 
p.celebrity_exclusive, category1_id, category2_id, category3_id, category4_id, category1, category2, category3, category4, COALESCE(c.first_online_date,p.first_online_date) first_online_date, 
COALESCE(p.is_disabled_now,c.is_disabled_now)is_disabled_now, COALESCE(p.current_enable_status,c.current_enable_status)current_enable_status, size, 
COALESCE(p.color,c.color) as color, price, coalesce(special_price,price) as special_price, COALESCE(c.image_url,p.image_url) as image_url,
category1_list, category2_list, category3_list, category4_list, category1_list_id, category2_list_id, category3_list_id, category4_list_id, 
base.child_id, base.parent_id
from dwh.sku_master_base base
join dwh.parent_props_temp p on base.parent_id=p.parent_id
join dwh.child_props_temp c on base.child_id=c.child_id;
-- 6.5 sec

-- To delete disabled test skus 
delete from dwh.sku_master where current_enable_status=2 AND (lower(trim(sku_name)) like '% test%' or lower(trim(sku_name)) like 'test%');

-- To delete duplicate entries
drop table if exists dwh.dup_sku_master_records;

create table dwh.dup_sku_master_records(index parent_id (parent_id)) select parent_id from
(select parent_id, parent_sku as sku, lpad(parent_sku,6,'0') as sku_clean, sku_name, sku_gender,created_date 
from dwh.sku_master parent_sku
where lpad(parent_sku,6,'0') in (select lpad(parent_sku,6,'0') new_sku from dwh.sku_master where length(parent_sku)<=6
    group by new_sku having count(DISTINCT parent_id) > 1)
group by 1) parent_sku
LEFT JOIN boutiqaat_v2.catalog_product_entity_int as cpei ON parent_sku.parent_id = cpei.row_id AND cpei.attribute_id in (96) AND cpei.store_id = 0
where cpei.value=2 and length(sku)<6;
-- 1.5 sec

delete from dwh.sku_master where parent_id in (select parent_id from dwh.dup_sku_master_records);

-- to get gender value from other store_ids
drop table if exists dwh.parent_props_gender_extra_temp;

create temporary table dwh.parent_props_gender_extra_temp(index parent_id (parent_id))
select parent_id, group_concat(distinct case when gender.value like '%4194,2741%' or gender.value like '%2741,4194%' then 'unisex' when gender.value like '%4194%' THEN 'women' when gender.value like '%2741%' THEN 'men' else gender.value end) as gender
FROM (select distinct parent_id from dwh.sku_master sm where sku_gender is null) as parent_sku
LEFT JOIN boutiqaat_v2.catalog_product_entity_text gender ON parent_sku.parent_id = gender.row_id and attribute_id = 542 
group by 1 having gender is not null;

update dwh.sku_master sm join dwh.parent_props_gender_extra_temp extra on sm.parent_id = extra.parent_id set sm.sku_gender=extra.gender where sm.sku_gender is null;

-- to get gender from child level
drop table if exists dwh.parent_props_gender_child_temp;

create temporary table dwh.parent_props_gender_child_temp(PRIMARY KEY child_id (child_id)) 
select child_id, group_concat(distinct case when gender.value like '%4194,2741%' or gender.value like '%2741,4194%' then 'unisex' when gender.value like '%4194%' THEN 'women' when gender.value like '%2741%' THEN 'men' else gender.value end) as gender
FROM (select distinct child_id from dwh.sku_master sm where sku_gender is null) as master
LEFT JOIN boutiqaat_v2.catalog_product_entity_text gender ON master.child_id = gender.row_id and attribute_id = 542 
group by 1 having gender is not null;

update dwh.sku_master sm join dwh.parent_props_gender_child_temp child_gender on sm.child_id = child_gender.child_id set sm.sku_gender=child_gender.gender where sm.sku_gender is null;

-- to get brand values from other store_ids
drop table if exists dwh.parent_props_brand_child_temp;

create temporary table dwh.parent_props_brand_child_temp (PRIMARY KEY child_id (child_id)) select prod.*, brand_english_name as brand from (select child_id, 
group_concat(distinct case when cpei.attribute_id=81 then cpei.value end) as brand_id
FROM (select distinct child_id from dwh.sku_master sm where brand_id is null or brand is null) as child_sku
LEFT JOIN boutiqaat_v2.catalog_product_entity_int as cpei ON child_sku.child_id = cpei.row_id AND cpei.attribute_id in (81) group by 1) as prod
LEFT JOIN boutiqaat_v2.brand_management bm on prod.brand_id = bm.brand_option_id
where brand_english_name is not null;

update dwh.sku_master sm join dwh.parent_props_brand_child_temp extra on sm.child_id = extra.child_id set sm.brand_id=extra.brand_id, sm.brand=extra.brand where sm.brand_id is null or sm.brand is null;

-- to get category values by looking uo with child row_id
drop table if exists dwh.child_props_category_temp;
SET @row_number = 0, @p_id= 0;

create temporary table dwh.child_props_category_temp (PRIMARY KEY child_id (child_id)) 
select child_id, 
CASE WHEN level >= 3 THEN cat1_name ELSE ''
END AS category1,
CASE WHEN level >= 4 THEN cat2_name ELSE ''
END AS category2,
CASE WHEN level >= 5 THEN cat3_name ELSE ''
END AS category3,
CASE WHEN level >= 6 THEN cat4_name ELSE ''
END AS category4,
CASE WHEN level >= 3 THEN cat1_id ELSE ''
END AS category1_id,
CASE WHEN level >= 4 THEN cat2_id ELSE ''
END AS category2_id,
CASE WHEN level >= 5 THEN cat3_id ELSE ''
END AS category3_id,
CASE WHEN level >= 6 THEN cat4_id ELSE ''
END AS category4_id,
CONCAT( '--', GROUP_CONCAT(DISTINCT CASE WHEN level >= 3 THEN cat1_name ELSE '' END ORDER BY cat1_id ASC SEPARATOR '--'), '--') AS category1_list,
CONCAT( '--', GROUP_CONCAT(DISTINCT CASE WHEN level >= 4 THEN cat2_name ELSE ''
END ORDER BY cat2_id ASC SEPARATOR '--'), '--')  AS category2_list,
CONCAT( '--', GROUP_CONCAT(DISTINCT CASE WHEN level >= 5 THEN cat3_name ELSE ''
END ORDER BY cat3_id ASC SEPARATOR '--'), '--')  AS category3_list,
CONCAT( '--', GROUP_CONCAT(DISTINCT CASE WHEN level >= 6 THEN cat4_name ELSE ''
END ORDER BY cat4_id ASC SEPARATOR '--'), '--')  AS category4_list,
CONCAT( '--', GROUP_CONCAT(DISTINCT CASE WHEN level >= 3 THEN cat1_id ELSE ''
END ORDER BY cat1_id ASC SEPARATOR '--'), '--')  AS category1_list_id,
CONCAT( '--', GROUP_CONCAT(DISTINCT CASE WHEN level >= 4 THEN cat2_id ELSE ''
END ORDER BY cat2_id ASC SEPARATOR '--'), '--')  AS category2_list_id,
CONCAT( '--', GROUP_CONCAT(DISTINCT CASE WHEN level >= 5 THEN cat3_id ELSE ''
END ORDER BY cat3_id ASC SEPARATOR '--'), '--')  AS category3_list_id,
CONCAT( '--', GROUP_CONCAT(DISTINCT CASE WHEN level >= 6 THEN cat4_id ELSE ''
END ORDER BY cat4_id ASC SEPARATOR '--'), '--')  AS category4_list_id
FROM 
(select @row_number:=CASE WHEN @p_id=child_id THEN @row_number + 1 ELSE 1 END AS row_num, @p_id:=child_id, ordered_cats.*
from (select child_sku.child_id, cce.level, cce.`position`, cce.path, cat1_name.row_id cat1_id, cat2_name.row_id cat2_id,  cat3_name.row_id cat3_id, cat4_name.row_id cat4_id, cat1_name.value cat1_name, cat2_name.value cat2_name, cat3_name.value cat3_name, cat4_name.value cat4_name
FROM (select distinct child_id from dwh.sku_master where category1 is null OR category2 is null ) as child_sku
JOIN boutiqaat_v2.catalog_category_product ccp ON child_sku.child_id = ccp.product_id 
JOIN boutiqaat_v2.catalog_category_entity AS cce ON ccp.category_id = cce.entity_id and level >= 3
JOIN boutiqaat_v2.catalog_category_entity_varchar AS cat1_name ON SUBSTRING_INDEX(SUBSTRING_INDEX(cce.path, '/', 4),'/',-1) = cat1_name.row_id AND cat1_name.attribute_id = 41 AND cat1_name.store_id = 0
JOIN boutiqaat_v2.catalog_category_entity_varchar AS cat2_name ON SUBSTRING_INDEX(SUBSTRING_INDEX(cce.path, '/', 5),'/',-1) = cat2_name.row_id AND cat2_name.attribute_id = 41 AND cat2_name.store_id = 0
JOIN boutiqaat_v2.catalog_category_entity_varchar AS cat3_name ON SUBSTRING_INDEX(SUBSTRING_INDEX(cce.path, '/', 6),'/',-1) = cat3_name.row_id AND cat3_name.attribute_id = 41 AND cat3_name.store_id = 0
JOIN boutiqaat_v2.catalog_category_entity_varchar AS cat4_name ON SUBSTRING_INDEX(SUBSTRING_INDEX(cce.path, '/', 7),'/',-1) = cat4_name.row_id AND cat4_name.attribute_id = 41 AND cat4_name.store_id = 0
WHERE
(IF(LOCATE(2741,cce.path)>0,1,0) OR
IF(LOCATE(4194,cce.path)>0,1,0)) AND cce.level >= 3
order by child_id, cce.level desc, cce.position desc, cce.path desc
) ordered_cats) ranked_cats
where row_num=1
group by 1;
-- 9 sec

update dwh.child_props_category_temp cat JOIN (select child_id,
CONCAT( '--', GROUP_CONCAT(DISTINCT CASE WHEN level >= 3 THEN cat1_name ELSE '' END ORDER BY cat1_id ASC SEPARATOR '--'), '--') AS category1_list,
CONCAT( '--', GROUP_CONCAT(DISTINCT CASE WHEN level >= 4 THEN cat2_name ELSE ''
END ORDER BY cat2_id ASC SEPARATOR '--'), '--')  AS category2_list,
CONCAT( '--', GROUP_CONCAT(DISTINCT CASE WHEN level >= 5 THEN cat3_name ELSE ''
END ORDER BY cat3_id ASC SEPARATOR '--'), '--')  AS category3_list,
CONCAT( '--', GROUP_CONCAT(DISTINCT CASE WHEN level >= 6 THEN cat4_name ELSE ''
END ORDER BY cat4_id ASC SEPARATOR '--'), '--')  AS category4_list,
CONCAT( '--', GROUP_CONCAT(DISTINCT CASE WHEN level >= 3 THEN cat1_id ELSE ''
END ORDER BY cat1_id ASC SEPARATOR '--'), '--')  AS category1_list_id,
CONCAT( '--', GROUP_CONCAT(DISTINCT CASE WHEN level >= 4 THEN cat2_id ELSE ''
END ORDER BY cat2_id ASC SEPARATOR '--'), '--')  AS category2_list_id,
CONCAT( '--', GROUP_CONCAT(DISTINCT CASE WHEN level >= 5 THEN cat3_id ELSE ''
END ORDER BY cat3_id ASC SEPARATOR '--'), '--')  AS category3_list_id,
CONCAT( '--', GROUP_CONCAT(DISTINCT CASE WHEN level >= 6 THEN cat4_id ELSE ''
END ORDER BY cat4_id ASC SEPARATOR '--'), '--')  AS category4_list_id
FROM
(select child_sku.child_id child_id, cce.level, cce.`position`, cce.path, cat1_name.row_id cat1_id, cat2_name.row_id cat2_id,  cat3_name.row_id cat3_id, cat4_name.row_id cat4_id, cat1_name.value cat1_name, cat2_name.value cat2_name, cat3_name.value cat3_name, cat4_name.value cat4_name
FROM (select distinct child_id from dwh.sku_master where category1 is null or category2 is null) as child_sku
JOIN boutiqaat_v2.catalog_category_product ccp ON child_sku.child_id = ccp.product_id 
JOIN boutiqaat_v2.catalog_category_entity AS cce ON ccp.category_id = cce.entity_id and level >= 3
JOIN boutiqaat_v2.catalog_category_entity_varchar AS cat1_name ON SUBSTRING_INDEX(SUBSTRING_INDEX(cce.path, '/', 4),'/',-1) = cat1_name.row_id AND cat1_name.attribute_id = 41 AND cat1_name.store_id = 0
JOIN boutiqaat_v2.catalog_category_entity_varchar AS cat2_name ON SUBSTRING_INDEX(SUBSTRING_INDEX(cce.path, '/', 5),'/',-1) = cat2_name.row_id AND cat2_name.attribute_id = 41 AND cat2_name.store_id = 0
JOIN boutiqaat_v2.catalog_category_entity_varchar AS cat3_name ON SUBSTRING_INDEX(SUBSTRING_INDEX(cce.path, '/', 6),'/',-1) = cat3_name.row_id AND cat3_name.attribute_id = 41 AND cat3_name.store_id = 0
JOIN boutiqaat_v2.catalog_category_entity_varchar AS cat4_name ON SUBSTRING_INDEX(SUBSTRING_INDEX(cce.path, '/', 7),'/',-1) = cat4_name.row_id AND cat4_name.attribute_id = 41 AND cat4_name.store_id = 0
WHERE
(IF(LOCATE(2741,cce.path)>0,1,0) OR
IF(LOCATE(4194,cce.path)>0,1,0)) AND cce.level >= 3) all_products
group by 1) cat_list ON cat_list.child_id = cat.child_id
SET cat.category1_list=cat_list.category1_list, cat.category2_list=cat_list.category2_list, cat.category3_list=cat_list.category3_list, cat.category4_list=cat_list.category4_list,
cat.category1_list_id=cat_list.category1_list_id, cat.category2_list_id=cat_list.category2_list_id, cat.category3_list_id=cat_list.category3_list_id, cat.category4_list_id=cat_list.category4_list_id
WHERE 1=1;
-- 12 sec

update dwh.sku_master master join dwh.child_props_category_temp as new_cat on master.child_id=new_cat.child_id
set master.category1 = new_cat.category1, master.category2 = new_cat.category2, master.category3 = new_cat.category3, master.category4 = new_cat.category4,
master.category1_id = new_cat.category1_id, master.category2_id = new_cat.category2_id, master.category3_id = new_cat.category3_id, master.category4_id = new_cat.category4_id,
master.category1_list = new_cat.category1_list, master.category2_list = new_cat.category2_list, master.category3_list = new_cat.category3_list, master.category4_list = new_cat.category4_list,
master.category1_list_id = new_cat.category1_list_id, master.category2_list_id = new_cat.category2_list_id, master.category3_list_id = new_cat.category3_list_id, master.category4_list_id = new_cat.category4_list_id
where master.category1 is null or master.category2 is null;

