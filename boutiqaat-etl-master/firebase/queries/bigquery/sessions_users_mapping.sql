SELECT
  COALESCE(device.advertising_id,device.vendor_id)  advertising_id,
  (SELECT value.string_value FROM UNNEST(user_properties)WHERE key = 'email') email,
  (SELECT value.string_value FROM UNNEST(user_properties) WHERE key = 'first_name') first_name,
  (SELECT value.string_value FROM UNNEST(user_properties)WHERE key = 'last_name') last_name,
  (SELECT value.string_value FROM UNNEST(user_properties)WHERE key = 'gender') gender,
  geo.country,
  geo.city
FROM `{{TABLE}}` AS T
group by 1,2,3,4,5,6,7