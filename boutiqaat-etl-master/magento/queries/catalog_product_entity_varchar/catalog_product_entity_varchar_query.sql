SELECT
    value_id,
    attribute_id,
    store_id,
    row_id,
    REPLACE(REPLACE(REPLACE(REPLACE(TRIM(value), '\r', ''),'\n',''),'\t',''),'\"', '') as value
FROM boutiqaat_v2.catalog_product_entity_varchar
WHERE \$CONDITIONS