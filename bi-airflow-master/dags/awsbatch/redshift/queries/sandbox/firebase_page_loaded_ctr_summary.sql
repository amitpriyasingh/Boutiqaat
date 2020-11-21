BEGIN;
DROP TABLE IF EXISTS sandbox.firebase_page_loaded_ctr_summary;
SELECT * INTO sandbox.firebase_page_loaded_ctr_summary FROM
(
    select 
        Date(event_date_kwt) as event_date,
        impressions,
        clicks,
        platform,
        app_version,
        plp_page
    from firebase.page_loaded_ctr_summary
    where Date(event_date_kwt) between (CURRENT_DATE - INTERVAL '8 DAY') and (CURRENT_DATE - INTERVAL '1 DAY') 
    group by 1,2,3,4,5,6
    ORDER BY Date(event_date_kwt)DESC
);
COMMIT;