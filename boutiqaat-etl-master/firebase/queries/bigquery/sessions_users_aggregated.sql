SELECT
    advertising_id,
    email,
    first_name,
    last_name,
    gender,
    country, 
    city 
FROM `boutiqaat-online-shopping.firebase.sessions_users` 
GROUP BY 1,2,3,4,5,6,7