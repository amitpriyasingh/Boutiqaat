{% set FDATE = "{}-{}-{}".format(DATE[:4], DATE[4:6], DATE[6:]) %}
BEGIN;
DELETE FROM adjust.transactions WHERE ingestion_date='{{FDATE}}';
INSERT INTO adjust.transactions
SELECT  
	tracking_year, 
	tracking_month, 
	tracking_date, 
	ingestion_date,
	created_at||nonce||tracker||activity_kind as tx_unique_id,
	COALESCE(adid, idfa_android_id, idfa_gps_adid_fire_adid,idfv) as user_id,
	DATE(TIMESTAMP WITH TIME ZONE 'epoch' + created_at * INTERVAL '1 second') order_date,
	(TIMESTAMP WITH TIME ZONE 'epoch' + created_at * INTERVAL '1 second') order_at,
	case 
		when is_valid_json(partner_parameters) = 1 
		then json_extract_path_text(partner_parameters,'transaction_id') 
		else NULL 
	end as tx_number,
	country,
	tracker as adj_tracker,
	tracker_name as adj_tracker_name,
	first_tracker as adj_first_tracker,
	last_tracker as adj_last_tracker,
	last_tracker_name as adj_last_tracker_name,
	outdated_tracker as adj_outdated_tracker,
	network_name as adj_network_name,
	campaign_name as adj_campaign_name,
	adgroup_name as adj_adgroup_name,
	creative_name as adj_creative_name,
	impression_based as adj_impression_based,
	is_organic as adj_is_organic,
	rejection_reason as adj_rejection_reason,
	click_referer as adj_click_referer,
	match_type as adj_match_type,
	reftag as adj_reftag,
	referrer as adj_referrer,
	revenue_float as bi_revenue_float,
	revenue as bi_revenue,
	revenue_usd as bi_revenue_usd,
	coalesce(idfa_gps_adid_fire_adid,idfv,idfa_android_id,adid) as device_id
FROM spectrum.adjust_raw_data 
WHERE ingestion_date = '{{FDATE}}'
AND activity_kind='event' 
AND event_name='Transaction';
COMMIT;
