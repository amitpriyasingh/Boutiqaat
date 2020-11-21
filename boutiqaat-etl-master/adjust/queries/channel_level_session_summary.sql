{% set FDATE = "{}-{}-{}".format(DATE[:4], DATE[4:6], DATE[6:]) %}
BEGIN;
DROP TABLE IF EXISTS tmp_channel_level_session_summary;
CREATE TEMP TABLE tmp_channel_level_session_summary AS
WITH sessions_activity as
(
    select 
        device_id,
        network_name, 
        campaign_name, 
        adgroup_name,
        creative_name,
        activity,
        activity_at,
        row_number() over (partition by device_id order by activity_at desc ) as rank
    from 
    (
        select 
            tracking_date,
            ingestion_date,
            device_id,
            tx_unique_id,
            country,
            activity,
            activity_at,
            network_name,
            campaign_name,
            adgroup_name,
            creative_name 
        from adjust.attributes_actions
        where tracking_date between DATE('{{FDATE}}') - INTERVAL '7 DAY' and  '{{FDATE}}'
        group by 1,2,3,4,5,6,7,8,9,10,11
    )
)
SELECT 
    x1.activity_id,
    x1.device_id,
    Date(x1.activity_at) activity_date,
    x1.activity_at,
    x1.country,
    x1.network_name,
    x1.campaign_name,
    x1.adgroup_name,
    x1.creative_name,
    Date(x1.bi_attr_activity_at_7days) bi_attr_activity_date_7days,
    x1.bi_attr_activity_at_7days,
    x1.bi_attr_activity_7days,
    x1.bi_network_name_7days,
    x1.bi_campaign_name_7days,
    x1.bi_adgroup_name_7days,
    x1.bi_creative_name_7days,
    x2.network_name_final as bi_network_group_name_7days,
    Date(x1.bi_attr_activity_at_48hours) bi_attr_activity_date_48hours,
    x1.bi_attr_activity_at_48hours,
    x1.bi_attr_activity_48hours,
    x1.bi_network_name_48hours,
    x1.bi_campaign_name_48hours,
    x1.bi_adgroup_name_48hours,
    x1.bi_creative_name_48hours,
    x3.network_name_final as bi_network_group_name_48hours
FROM
(
    select
        a.*
    from 
	(            
		select 
			sessions.*,
			COALESCE(attr_actions_7days.network_name,'Organic') as bi_network_name_7days,
			attr_actions_7days.campaign_name as bi_campaign_name_7days,
			attr_actions_7days.adgroup_name as bi_adgroup_name_7days,
			attr_actions_7days.creative_name as bi_creative_name_7days,
			attr_actions_7days.activity as bi_attr_activity_7days,
			attr_actions_7days.activity_at as bi_attr_activity_at_7days,
			COALESCE(attr_actions_48hours.network_name,'Organic') as bi_network_name_48hours,
			attr_actions_48hours.campaign_name as bi_campaign_name_48hours,
			attr_actions_48hours.adgroup_name as bi_adgroup_name_48hours,
			attr_actions_48hours.creative_name as bi_creative_name_48hours,
			attr_actions_48hours.activity as bi_attr_activity_48hours,
			attr_actions_48hours.activity_at as bi_attr_activity_at_48hours
		from (select * from adjust.daily_sessions where tracking_date='{{FDATE}}') as sessions
		left join (select * from sessions_activity where rank=1) as attr_actions_7days
		on sessions.device_id = attr_actions_7days.device_id 
        and attr_actions_7days.activity_at between (sessions.activity_at - INTERVAL '7 days') and (sessions.activity_at + INTERVAL '1 hour') 
		left join (select * from sessions_activity where rank=1) as attr_actions_48hours 
		on sessions.device_id = attr_actions_48hours.device_id 
        and attr_actions_48hours.activity_at between (sessions.activity_at - INTERVAL '2 days') and (sessions.activity_at + INTERVAL '1 hour')
	) a 
) as x1
LEFT JOIN sandbox.network_grouped x2 
ON x2.network_name = x1.bi_network_name_7days
LEFT JOIN sandbox.network_grouped x3 
ON x3.network_name = x1.bi_network_name_48hours;
	

delete from adjust.channel_level_session_summary where activity_date='{{FDATE}}';
INSERT INTO adjust.channel_level_session_summary
SELECT *  FROM tmp_channel_level_session_summary;

COMMIT;

