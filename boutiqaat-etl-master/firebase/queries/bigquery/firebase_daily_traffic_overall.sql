-- insert into `boutiqaat-online-shopping.firebase.daily_traffic`
select 
    event_date_kwt, 
    platform, 
    app_version,
    CASE WHEN store_country='KW' THEN 'Kuwait' 
	    WHEN store_country='SA' THEN 'Saudi Arabia' 
	    WHEN store_country='AE' THEN 'United Arab Emirates' 
	    WHEN store_country='OM' THEN 'Oman' 
	    WHEN store_country='BH' THEN 'Bahrain' 
	    WHEN store_country='QA' THEN 'Qatar' 
	    WHEN geo_country IN ('Kuwait','Saudi Arabia','United Arab Emirates','Oman','Bahrain','Qatar') THEN geo_country
	    ELSE 'Other' 
    END as country,
    'Celebrity' as page_type, 
    '0' as page_id, 
    'Boutiqaat Overall' as page_name, 
    COUNT(DISTINCT user_pseudo_id) total_uniq_users,
    COUNT(DISTINCT CASE WHEN secondary_int_src_type IN ('na','') THEN CASE WHEN primary_int_src_type NOT IN ('na','') THEN user_pseudo_id ELSE NULL END ELSE NULL END) primary_uniq_users,
    COUNT(DISTINCT CASE WHEN secondary_int_src_type NOT IN ('na','') THEN user_pseudo_id ELSE NULL END) secondary_uniq_users,
    count(1) total_page_loads,
    SUM(CASE WHEN secondary_int_src_type IN ('na','') THEN CASE WHEN primary_int_src_type NOT IN ('na','') THEN 1 ELSE 0 END ELSE 0 END) primary_page_loads,
    SUM(CASE WHEN secondary_int_src_type NOT IN ('na','') THEN 1 ELSE 0 END) secondary_page_loads
FROM
(
    select 
        CAST(DATETIME(TIMESTAMP_MICROS(event_timestamp),"+03:00") as DATE) as event_date_kwt, 
        platform, 
        app_info.version app_version,
        user_pseudo_id, 
        geo.country geo_country, 
        (SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'store_country') store_country,
        (SELECT value.string_value FROM UNNEST(event_params)WHERE key = 'secondary_int_src_type') secondary_int_src_type,
        (SELECT value.string_value FROM UNNEST(event_params)WHERE key = 'primary_int_src_type') primary_int_src_type
    FROM `{{TABLE}}`
    WHERE COALESCE((SELECT value.string_value FROM UNNEST(event_params)WHERE key = 'PageType'),
            (SELECT value.string_value FROM UNNEST(event_params)WHERE key = 'page_type')) 
        in ('Listing Page','Search_result Page') 
)pages
group by 1,2,3,4,5,6,7;