{% set FDATE = "{}-{}-{}".format(DATE[:4], DATE[4:6], DATE[6:]) %}
BEGIN;
DELETE FROM adjust.attributes_actions WHERE ingestion_date='{{FDATE}}';
INSERT INTO adjust.attributes_actions
SELECT 
    ingestion_date, 
    tracking_date, 
    tracking_month,
    tracking_year, 
    created_at||nonce||tracker||activity_kind AS tx_unique_id, 
    COALESCE(adid, idfa_android_id, idfa_gps_adid_fire_adid,idfv) AS user_id,
    activity_kind as activity,
    (timestamp with time zone 'epoch' + created_at * interval '1 second') AS activity_at, 
    match_type,
    network_name, 
    campaign_name,
    adgroup_name,
    creative_name,
    tracker,
    last_tracker_name,
    tracker_name,
    is_organic,
    country,
    coalesce(idfa_gps_adid_fire_adid,idfv,idfa_android_id,adid) AS device_id,
    count(1) records
FROM spectrum.adjust_raw_data 
WHERE ingestion_date = '{{FDATE}}'
AND activity_kind in ('install','install_update','click','reattribution_update','reinstall','reattribution','reattribution_reinstall') 
GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19;
COMMIT;
