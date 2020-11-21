SELECT
  app_version,
  event_name,
  session_id,
  ga_session_number,
  advertising_id,
  PARSE_DATE('%Y%m%d',event_date) as event_date,
  event_datetime,
  page_id,
  catalog_page_type,
  sku
FROM
  `boutiqaat-online-shopping.firebase.fact_impressions`
WHERE
  DATE(_PARTITIONTIME) = PARSE_DATE('%Y%m%d','{{DATE}}') 
  and event_name='view_item' 
  and sku is not null