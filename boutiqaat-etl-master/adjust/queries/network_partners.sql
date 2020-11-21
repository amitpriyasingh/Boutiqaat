{% set FDATE = "{}-{}-{}".format(DATE[:4], DATE[4:6], DATE[6:]) %}
BEGIN;
DELETE FROM adjust.network_partners WHERE ingestion_date='{{FDATE}}';
INSERT INTO adjust.network_partners
SELECT 
    tracking_year,
    tracking_month,
    tracking_date,
    ingestion_date,
    created_at||nonce||tracker||activity_kind as tx_unique_id,
    COALESCE(adid, idfa_android_id, idfa_gps_adid_fire_adid,idfv) as user_id,
    activity_kind as activity, 
    (TIMESTAMP WITH TIME ZONE 'epoch' + created_at * INTERVAL '1 second') as activity_at, 
    match_type, 
    network_name, 
    campaign_name, 
    adgroup_name,
    creative_name,
    last_tracker_name, 
    tracker_name, 
    app_version_short, 
    country,
    coalesce(idfa_gps_adid_fire_adid,idfv,idfa_android_id,adid) as device_id,
    count(1) records
FROM spectrum.adjust_raw_data
WHERE ingestion_date='{{FDATE}}'
AND network_name!='Organic' 
GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18;

COMMIT;
