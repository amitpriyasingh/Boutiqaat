SELECT
    row_id, 
    entity_id, 
    created_in, 
    updated_in, 
    attribute_set_id, 
    type_id, 
    sku, 
    has_options, 
    required_options, 
    created_at, 
    updated_at
FROM boutiqaat_v2.catalog_product_entity
WHERE \$CONDITIONS