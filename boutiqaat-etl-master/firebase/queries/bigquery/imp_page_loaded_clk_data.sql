select 
    k.* 
from 
(
    select 
        event_date_kwt, 
        user_pseudo_id, 
        platform, 
        app_version, 
        (CASE WHEN event_name="product_impression" THEN 'impression' WHEN event_name="page_loaded" THEN 'click' ELSE NULL END) as action, 
        CASE 
            WHEN event_name='page_loaded' THEN
	            CASE 
	                WHEN lower(previous_page_type) like '%celebrity%' THEN 'Celebrity'
	                WHEN lower(previous_page_type) like '%brand%' THEN 'Brand'
                    WHEN lower(previous_page_type) like '%category%' THEN 'Category'
                    WHEN lower(previous_page_type) like '%search%' THEN 'Search'
                    WHEN lower(previous_page_type) like '%tv%' THEN 'TV'
	                ELSE NULL
	            END
            WHEN event_name='product_impression' THEN 
	            CASE
                    WHEN lower(item_list) like '%celebrity%' THEN 'Celebrity'
                    WHEN lower(item_list) like '%brand%' THEN 'Brand'
                    WHEN lower(item_list) like '%category%' THEN 'Category'
                    WHEN lower(item_list) like '%search%' THEN 'Search'
                    WHEN lower(item_list) like '%tv%' THEN 'TV'
                    ELSE NULL
	            END
            ELSE NULL	
        END as plp_page,
        product_id, 
        event_timestamp ts, 
        LAG(event_timestamp) OVER(PARTITION BY product_id, user_pseudo_id ORDER BY event_timestamp) AS prev_ts 
    from
    (
        select 
            CAST(DATETIME(TIMESTAMP_MICROS(event_timestamp),"+03:00") as DATE) as event_date_kwt, 
            event_timestamp, 
            user_pseudo_id, 
            platform, 
            app_info.version as app_version, 
            event_name, 
            (SELECT value.string_value FROM UNNEST(event_params) WHERE key='secondary_int_src_type') secondary_int_src_type, 
            (SELECT value.string_value FROM UNNEST(event_params) WHERE key in ('previous_page_type','Previous_Page_Type','PreviousPageType')) previous_page_type, 
            replace(replace((SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'item_list'),'"',''),' ','')  as item_list, 
            CASE 
                WHEN event_name='page_loaded' THEN (SELECT value.string_value FROM UNNEST(event_params) WHERE key='secondary_int_src_id')
                WHEN event_name='product_impression'THEN  (SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'product_id')
                ELSE NULL
            END as product_id
        FROM `{{TABLE}}` 
        where app_info.version in ('5.8.3','5.8.4','5.8.5','5.8.6') and event_name in ('product_impression','page_loaded')
    ) ctr_base
    where (lower(secondary_int_src_type) like '%detail%' or event_name='product_impression') 
)k 
where ((ts - prev_ts)/1000000) > 5  and plp_page is not null and product_id is not null;