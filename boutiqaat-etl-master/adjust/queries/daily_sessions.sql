{% set FDATE = "{}-{}-{}".format(DATE[:4], DATE[4:6], DATE[6:]) %}
BEGIN;
DELETE FROM adjust.daily_sessions WHERE ingestion_date ='{{FDATE}}';
INSERT INTO adjust.daily_sessions
select 
tracking_date,
ingestion_date,
created_at||nonce||tracker||activity_kind as activity_id,
coalesce(idfa_gps_adid_fire_adid,idfv,idfa_android_id,adid) as device_id,
(TIMESTAMP WITH TIME ZONE 'epoch' + created_at * INTERVAL '1 second') as activity_at, 
country, 
network_name,
campaign_name,
adgroup_name,
creative_name
from spectrum.adjust_raw_data 
WHERE ingestion_date = '{{FDATE}}'
AND activity_kind='session'
group by 1,2,3,4,5,6,7,8,9,10;

COMMIT;