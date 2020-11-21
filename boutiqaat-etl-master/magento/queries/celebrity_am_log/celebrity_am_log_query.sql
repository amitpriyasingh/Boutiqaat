SELECT
    id,
    celebrity_id,
    celebrity_name,
    am_id,
    am_username,
    am_name,
    am_email,
    celebrity_am_startdate,
    celebrity_am_enddate,
    is_active,
    am_rm_name,
    am_rm_email
FROM boutiqaat_v2.celebrity_am_log
WHERE \$CONDITIONS