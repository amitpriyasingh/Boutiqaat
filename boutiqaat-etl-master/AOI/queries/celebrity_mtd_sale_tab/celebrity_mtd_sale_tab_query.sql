SELECT
    target_month,
    max_order_at,
    max_item_id,
    celebrity_id,
    REPLACE(REPLACE(REPLACE(REPLACE(TRIM(celebrity_name), '\r', ''),'\n',''),'\t',''),'\"', '') as celebrity_name,
    code,
    celeb_mtd_sale,
    celeb_mtd_target,
    celeb_total_target,
    REPLACE(REPLACE(REPLACE(REPLACE(TRIM(account_manager), '\r', ''),'\n',''),'\t',''),'\"', '') as account_manager,
    REPLACE(REPLACE(REPLACE(REPLACE(TRIM(am_email), '\r', ''),'\n',''),'\t',''),'\"', '') as am_email,
    REPLACE(REPLACE(REPLACE(REPLACE(TRIM(rm_email), '\r', ''),'\n',''),'\t',''),'\"', '') as rm_email,
    am_celeb_count
FROM aoi.celebrity_mtd_sale_tab
WHERE \$CONDITIONS