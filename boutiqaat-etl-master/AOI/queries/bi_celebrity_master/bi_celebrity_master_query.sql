SELECT 
    entry_id, 
    celebrity_id, 
    REPLACE(REPLACE(REPLACE(REPLACE(TRIM(celebrity_name), '\r', ''),'\n',''),'\t',''),'\"', '') as celebrity_name, 
    REPLACE(REPLACE(REPLACE(REPLACE(TRIM(current_am), '\r', ''),'\n',''),'\t',''),'\"', '') as current_am, 
    code, 
    REPLACE(REPLACE(REPLACE(REPLACE(TRIM(am_email), '\r', ''),'\n',''),'\t',''),'\"', '') as am_email, 
    REPLACE(REPLACE(REPLACE(REPLACE(TRIM(rm_name), '\r', ''),'\n',''),'\t',''),'\"', '') as rm_name, 
    REPLACE(REPLACE(REPLACE(REPLACE(TRIM(rm_email), '\r', ''),'\n',''),'\t',''),'\"', '') as rm_email, 
    am_effective_date, 
    am_mapping_end_date, 
    is_entry_active, 
    last_updated_utc,
    boutique_gender
FROM aoi.bi_celebrity_master
WHERE \$CONDITIONS