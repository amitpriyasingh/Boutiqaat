SELECT
    event_id,
    events_header_id,
    user_name,
    celebrity_name,
    celebrity_id,
    generic,
    event_portal,
    event_type,
    event_class,
    bq_post,
    total_post,
    event_date,
    TIMESTAMP(CONCAT(event_date, ' ', event_time)) as event_time,
    created_at,
    updated_at,
    event_hours,
    event_minutes,
    REPLACE(REPLACE(REPLACE(REPLACE(TRIM(remark), '\r', ''),'\n',''),'\t',''),'\"', '') as remark,
    labelid,
    skuid,
    product_entity_id
FROM events.emt_report
WHERE \$CONDITIONS