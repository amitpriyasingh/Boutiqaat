{% set FDATE = "{}-{}-{}".format(DATE[:4], DATE[4:6], DATE[6:]) %}
BEGIN;
DELETE FROM adjust.sessions_activity WHERE ingestion_date ='{{FDATE}}';
INSERT INTO adjust.sessions_activity
SELECT
    tracking_date,
    ingestion_date,
    coalesce(idfa_gps_adid_fire_adid,idfv,idfa_android_id,adid) as device_id,
    created_at||nonce||tracker||activity_kind as activity_id,
    country, 
    activity_kind as activity,
    (TIMESTAMP WITH TIME ZONE 'epoch' + created_at * INTERVAL '1 second') as activity_at, 
    network_name,
    campaign_name,
    adgroup_name,
    creative_name
from spectrum.adjust_raw_data
where ingestion_date ='{{FDATE}}'
AND activity_kind in ('install','install_update','click','reattribution_update','reinstall','reattribution','reattribution_reinstall')
group by 1,2,3,4,5,6,7,8,9,10,11;