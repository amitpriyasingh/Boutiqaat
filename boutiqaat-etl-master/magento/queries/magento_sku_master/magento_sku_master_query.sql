SELECT 
    cp.sku, 
    REPLACE(REPLACE(REPLACE(REPLACE(TRIM(sku_desc.sku_name), '\r', ''),'\n',''),'\t',''),'\"', '') as sku_name,
    sku_desc.gender,
    COALESCE(parent_child.config_sku,cp.sku) as config_sku,
    size_color.size,
    size_color.color,
    CASE WHEN COALESCE(c.is_exclusive,'No') = 'Yes' THEN 1 ELSE 0 END as is_exclusive_to_celebrity,
    size_color.is_exclusive_to_boutiqaat,
    COALESCE(d.value, 0) as special_price,
    enable.enable_date
FROM boutiqaat_v2.catalog_product_entity cp
LEFT JOIN 
(
    SELECT 
        cpev.row_id,
        COALESCE(SUBSTRING_INDEX(group_concat(cpev.value ORDER BY FIELD(cpev.store_id,1,0,3) asc SEPARATOR '*'),'*',1),
                SUBSTRING_INDEX(group_concat(cpev.value ORDER BY FIELD(cpev.store_id,1,0,3) asc SEPARATOR '*' ),'*',2),
                SUBSTRING_INDEX(group_concat(cpev.value ORDER BY FIELD(cpev.store_id,1,0,3) asc SEPARATOR '*'),'*',3)
                ) as sku_name,
        CASE 
            WHEN cpet.value='4194' THEN 'women' 
            WHEN cpet.value='2741' THEN 'men' 
            WHEN cpet.value in ('4194,2741','2741,4194') THEN 'unisex' 
            ELSE 'other' 
        END as gender
    FROM boutiqaat_v2.catalog_product_entity_varchar cpev
    LEFT JOIN boutiqaat_v2.catalog_product_entity_text cpet on cpet.row_id = cpev.row_id
    WHERE cpev.attribute_id =71 and cpet.attribute_id=542
    GROUP BY cpev.row_id,cpet.attribute_id
) sku_desc on sku_desc.row_id = cp.row_id
LEFT JOIN
(
    SELECT 
        child_row_id, 
        cpe.sku as config_sku 
    FROM 
    (
        SELECT 
            child_id as child_row_id,
            group_concat(distinct parent_id order by parent_id asc) parent_row_id 
        FROM boutiqaat_v2.catalog_product_relation 
        GROUP BY child_id
    ) as cpr 
    INNER JOIN boutiqaat_v2.catalog_product_entity cpe
    ON cpe.row_id = cpr.parent_row_id and cpe.type_id <> 'bundle'
    GROUP BY child_row_id
) parent_child on parent_child.child_row_id = cp.row_id
LEFT JOIN
(
    SELECT 
        row_id, 
        group_concat(distinct CASE WHEN attr_id IN (539,293,538,519,509,512,541) THEN attr_value ELSE NULL END) as size, 
        group_concat(distinct CASE WHEN attr_id IN (92) THEN attr_value ELSE NULL END) as color, 
        group_concat(distinct CASE WHEN attr_id IN (265) THEN exclusive_value ELSE NULL END) is_exclusive_to_boutiqaat 
    FROM
    (
        SELECT 
            row_id, 
            attr_id, 
            group_concat(distinct eaov.value) as attr_value,
            group_concat(sku_attr_value.option_id) as exclusive_value 
        FROM 
        (
            SELECT 
                row_id, 
                attribute_id as attr_id, 
                value as option_id 
            FROM boutiqaat_v2.catalog_product_entity_int 
            WHERE attribute_id in (539,293,538,519,509,512,541,92,265) 
            GROUP BY 1,2,3
        ) sku_attr_value 
        LEFT JOIN boutiqaat_v2.eav_attribute_option_value eaov 
        on eaov.option_id=sku_attr_value.option_id 
        and eaov.store_id in (0,1) 
        GROUP BY 1,2
    ) sku_attr_value GROUP BY 1
) as size_color on size_color.row_id = cp.row_id 
LEFT JOIN boutiqaat_v2.celebrity_product c on c.product_entity_id = cp.row_id 
LEFT JOIN 
(
    SELECT 
        row_id,
        min(value) as value  
    FROM boutiqaat_v2.catalog_product_entity_decimal 
    WHERE attribute_id =76 
    GROUP BY row_id
) as d on d.row_id = cp.row_id
LEFT JOIN 
(
    SELECT 
        sku, 
        min(created_date) as enable_date 
    FROM boutiqaat_v2.sku_status_history ssh  
    WHERE status = 1 
    GROUP BY sku
) as enable on enable.sku = cp.sku 
where \$CONDITIONS and cp.type_id <> 'bundle' and cp.sku is not null 
GROUP BY cp.sku
