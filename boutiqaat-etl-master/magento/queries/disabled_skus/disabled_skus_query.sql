SELECT
    id,
    sku,
    product_id,
    status,
    created_at,
    updated_at
FROM boutiqaat_v2.disabled_skus
WHERE \$CONDITIONS