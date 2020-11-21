BEGIN;

DELETE FROM sandbox.magento_sku_master WHERE 1=1;

INSERT INTO sandbox.magento_sku_master
SELECT 
    cp.sku, 
    REPLACE(REPLACE(REPLACE(REPLACE(TRIM(sku_desc.sku_name), '\r', ''),'\n',''),'\t',''),'\"', '') as sku_name,
    sku_desc.gender,
    parent_child.config_sku,
    size_color.size,
    size_color.color,
    CASE WHEN COALESCE(c.is_exclusive,'No') = 'Yes' THEN 1 ELSE 0 END as is_exclusive_to_celebrity,
    size_color.is_exclusive_to_boutiqaat,
    COALESCE(d.value, 0) as special_price,
    enabled.enable_date
FROM magento.catalog_product_entity cp
LEFT JOIN 
(
    SELECT 
        cpev.row_id,
        CASE 
            WHEN cpet.value='4194' THEN 'women' 
            WHEN cpet.value='2741' THEN 'men' 
            WHEN cpet.value in ('4194,2741','2741,4194') THEN 'unisex' 
            ELSE 'other' 
        END as gender,
        COALESCE(SPLIT_PART(LISTAGG(cpev.value,  '*') WITHIN GROUP (ORDER BY (CASE WHEN cpev.store_id=1 THEN 1 WHEN cpev.store_id=0 THEN 2 WHEN cpev.store_id=3 THEN 3 END)),'*',1),
                SPLIT_PART(LISTAGG(cpev.value,  '*') WITHIN GROUP (ORDER BY (CASE WHEN cpev.store_id=1 THEN 1 WHEN cpev.store_id=0 THEN 2 WHEN cpev.store_id=3 THEN 3 END)),'*',2),
                SPLIT_PART(LISTAGG(cpev.value,  '*') WITHIN GROUP (ORDER BY (CASE WHEN cpev.store_id=1 THEN 1 WHEN cpev.store_id=0 THEN 2 WHEN cpev.store_id=3 THEN 3 END)),'*',3)
                ) as sku_name
    FROM magento.catalog_product_entity_varchar cpev
    LEFT JOIN magento.catalog_product_entity_text cpet on cpet.row_id = cpev.row_id
    WHERE cpev.attribute_id =71 and cpet.attribute_id=542
    GROUP BY 1,2
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
            LISTAGG(distinct parent_id) WITHIN GROUP (order by parent_id asc) parent_row_id 
        FROM magento.catalog_product_relation 
        GROUP BY child_id
    ) as cpr 
    INNER JOIN magento.catalog_product_entity cpe
    ON cpe.row_id = cpr.parent_row_id and cpe.type_id <> 'bundle'
    GROUP BY 1,2
) parent_child on parent_child.child_row_id = cp.row_id
LEFT JOIN
(
    SELECT 
        row_id, 
        LISTAGG(distinct CASE WHEN attr_id IN (539,293,538,519,509,512,541) THEN attr_value ELSE NULL END) as size, 
        LISTAGG(distinct CASE WHEN attr_id IN (92) THEN attr_value ELSE NULL END) as color, 
        LISTAGG(distinct CASE WHEN attr_id IN (265) THEN exclusive_value ELSE NULL END) is_exclusive_to_boutiqaat 
    FROM
    (
        SELECT 
            row_id, 
            attr_id, 
            LISTAGG(distinct eaov.value) as attr_value,
            LISTAGG(sku_attr_value.option_id) as exclusive_value 
        FROM 
        (
            SELECT 
                row_id, 
                attribute_id as attr_id, 
                value as option_id 
            FROM magento.catalog_product_entity_int 
            WHERE attribute_id in (539,293,538,519,509,512,541,92,265) 
            GROUP BY 1,2,3
        ) sku_attr_value 
        LEFT JOIN magento.eav_attribute_option_value eaov 
        on eaov.option_id=sku_attr_value.option_id 
        and eaov.store_id in (0,1) 
        GROUP BY 1,2
    ) sku_attr_value GROUP BY 1
) as size_color on size_color.row_id = cp.row_id 
LEFT JOIN 
(
	select 
		ROW_NUMBER() OVER(PARTITION BY product_entity_id ORDER BY updated_at DESC) as rank, 
		product_entity_id, 
		is_exclusive, 
		product_status, 
		updated_at 
	from magento.celebrity_product WHERE product_status='Online'
) c on c.product_entity_id = cp.row_id and c.rank=1
LEFT JOIN 
(
    SELECT 
        row_id,
        min(value) as value  
    FROM magento.catalog_product_entity_decimal 
    WHERE attribute_id =76 
    GROUP BY row_id
) as d on d.row_id = cp.row_id
LEFT JOIN 
(
    SELECT 
        sku, 
        min(created_date) as enable_date 
    FROM magento.sku_status_history ssh  
    WHERE status = 1 
    GROUP BY sku
) as enabled on enabled.sku = cp.sku 
where cp.type_id <> 'bundle' and cp.sku is not null
group by 1,2,3,4,5,6,7,8,9,10;

COMMIT;