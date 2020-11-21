SELECT event_id, events_header_id, user_name, celebrity_name, celebrity_id, generic, event_portal, event_type, event_class, bq_post, total_post, event_date, event_time, created_at, updated_at, event_hours, event_minutes, remark, labelid, skuid, product_entity_id
FROM aoi.celebrity_emt_report
WHERE \$CONDITIONS