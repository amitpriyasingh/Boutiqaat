update `boutiqaat-online-shopping.firebase.daily_traffic` as t1 
set t1.page_name=t2.page_name 
FROM 
(
    SELECT 
        page_type, 
        page_id, 
        MIN(page_name) page_name 
    FROM `boutiqaat-online-shopping.firebase.daily_traffic` 
    WHERE page_name is not null and DATE(_PARTITIONTIME) = PARSE_DATE('%Y%m%d','{{DATE}}')
    group by 1,2
) as t2 
WHERE t1.page_type=t2.page_type and t1.page_id=t2.page_id and t1.page_name is null
and DATE(_PARTITIONTIME) = PARSE_DATE('%Y%m%d','{{DATE}}')