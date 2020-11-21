SELECT
  event_date,
  app_info.version as app_version,
  event_name,
  event_params.value.string_value as catalog_page_type,
  count(event_date) cnt
FROM
  `{{TABLE}}` AS T
CROSS JOIN
  T.event_params
WHERE
  event_name = 'product_impression'
  AND event_params.key = 'item_list' group by 1,2,3,4