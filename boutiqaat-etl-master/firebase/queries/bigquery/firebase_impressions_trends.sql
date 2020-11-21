SELECT  
PARSE_DATE('%Y%m%d',event_date) as event_date,
store,
store_id,
country,
platform,
app_version,
event_name,
catalog_page_type,
catalog_page_id,
catalog_page_name,
sku,
count(1) as impressions
FROM `boutiqaat-online-shopping.firebase.fact_impressions` 
WHERE DATE(_PARTITIONTIME) = PARSE_DATE('%Y%m%d','{{DATE}}')  
group by 1,2,3,4,5,6,7,8,9,10,11