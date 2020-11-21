select 
    impressions_part.event_date_kwt, 
    impressions_part.platform, 
    impressions_part.app_version, 
    impressions_part.plp_page, 
    impressions_part.product_id, 
    impressions, 
    COALESCE(clicks,0) as clicks, 
    COALESCE(clicks,0)/NULLIF(impressions,0) as ctr, 
    impression_uniq_visitors, 
    COALESCE(click_uniq_visitors,0) as click_uniq_visitors, 
    COALESCE(click_uniq_visitors,0)/NULLIF(impression_uniq_visitors,0) as ctr_uniq_visitors 
from 
(
    select 
        event_date_kwt, 
        platform, 
        app_version, 
        CAST(product_id as INT64) as product_id, 
        plp_page, 
        count(1) as impressions, 
        count(distinct user_pseudo_id) impression_uniq_visitors 
    from `boutiqaat-online-shopping.marketing.imp_page_loaded_clk_data` 
    where action='impression' group by 1,2,3,4,5
) impressions_part
left join 
(
    select 
        event_date_kwt, 
        platform, 
        app_version, 
        CAST(product_id as INT64) as product_id, plp_page, 
        count(1) as clicks, 
        count(distinct user_pseudo_id) click_uniq_visitors 
    from `boutiqaat-online-shopping.marketing.imp_page_loaded_clk_data` 
    where action='click' group by 1,2,3,4,5
) clicks_part 
on impressions_part.event_date_kwt=clicks_part.event_date_kwt 
and impressions_part.platform=clicks_part.platform 
and impressions_part.app_version=clicks_part.app_version 
and impressions_part.plp_page=clicks_part.plp_page
and impressions_part.product_id=clicks_part.product_id;