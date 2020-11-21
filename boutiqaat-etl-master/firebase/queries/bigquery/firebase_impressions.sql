SELECT
  CASE WHEN event_name like '%product_impression%' THEN 'product_impression'
        ELSE event_name
  END as event_name,
  event_date,
  app_info.version as app_version,
  event_timestamp,
  user_pseudo_id,
  device.advertising_id as advertising_id,
  COALESCE(MAX(IF(event_params.key='ga_session_id', event_params.value.int_value, NULL)), MAX(IF(event_params.key='ga_session_id', event_params.value.int_value, NULL))) ga_session_id,
  COALESCE(MAX(IF(event_params.key='ga_session_number', event_params.value.int_value, NULL)), MAX(IF(event_params.key='ga_session_number', event_params.value.int_value, NULL))) ga_session_number,
  COALESCE(MAX(IF(event_params.key='store', event_params.value.string_value, NULL)), MAX(IF(event_params.key='store', event_params.value.string_value, NULL))) store,
  COALESCE(MAX(IF(event_params.key='store_id', event_params.value.string_value, NULL)), MAX(IF(event_params.key='store_id', event_params.value.string_value, NULL))) store_id,
  COALESCE(MAX(IF(event_params.key='store_country', event_params.value.string_value, NULL)), MAX(IF(event_params.key='store_country', event_params.value.string_value, NULL))) country,
  COALESCE(MAX(IF(event_params.key='platform', event_params.value.string_value, NULL)), MAX(IF(event_params.key='platform', event_params.value.string_value, NULL))) platform,
  COALESCE(MAX(IF(event_params.key='item_list', event_params.value.string_value, NULL)), MAX(IF(event_params.key='item_list', event_params.value.string_value, NULL))) catalog_page_type,
  COALESCE(MAX(IF(event_params.key='list_id', event_params.value.string_value, NULL)), MAX(IF(event_params.key='list_id', event_params.value.string_value, NULL))) catalog_page_id,
  COALESCE(MAX(IF(event_params.key='list_name', event_params.value.string_value, NULL)), MAX(IF(event_params.key='firebase_screen', event_params.value.string_value, NULL))) catalog_page_name,
  COALESCE(MAX(IF(event_params.key='ListOwner', event_params.value.string_value, NULL)), MAX(IF(event_params.key='ListOwner', event_params.value.string_value, NULL))) page_id,
  COALESCE(MAX(IF(event_params.key='item_id', event_params.value.string_value, NULL)), MAX(IF(event_params.key='sku', event_params.value.string_value, NULL))) sku,
FROM `{{TABLE}}` AS T
CROSS JOIN T.event_params
where event_name in ("mkt_product_impression","product_impression", "view_item")
GROUP BY 1,2,3,4,5,6