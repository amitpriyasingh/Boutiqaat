select DATE(FROM_UNIXTIME(created_time)) report_date_utc, currency as country, device_type,
case when payment='myfatoorah_vm' then 'MyFatoorah Visa Master' 
     when payment='myfatoorah_s' then 'MyFatoorah Sada' 
     when payment='myfatoorah_np' then 'MyFatoorah Qpay'
     when payment='myfatoorah_b' then 'MyFatoorah Benefit'
     when payment='myfatoorah_kn' then 'MyFatoorah KNET'
     when payment='knet' then 'KNET'
     when payment='md_cybersourcesa' then 'Cybersource Visa Master'
     else payment
end as payment_gateway,
count(distinct increment_id) as total_tx,
count(distinct case when result in ('ACCEPT','CAPTURED') then increment_id else NULL end) as success_tx,
count(distinct case when result in ('NOT CAPTURED','DECLINE','FAILURE(NOT CAPTURED)','failed','FAILURE(SUSPECT)','ERROR','FAILURE(HOST TIMEOUT)','FAILURE(DENIED BY RISK)','HOST TIMEOUT','CANCEL','CANCELED') then increment_id else NULL end) as fail_tx,
count(distinct case when TRIM(coalesce(result,'')) in ('User_Closed','') then increment_id else NULL end) as user_closed_tx,
count(distinct case when TRIM(coalesce(result,'')) not in ('ACCEPT','CAPTURED','NOT CAPTURED','DECLINE','FAILURE(NOT CAPTURED)','failed','FAILURE(SUSPECT)','ERROR','FAILURE(HOST TIMEOUT)','FAILURE(DENIED BY RISK)','HOST TIMEOUT','CANCEL','CANCELED','User_Closed','') then increment_id else NULL end) as other_tx,
max(last_updated) last_updated
from boutiqaat_middlelayer.order_queue 
where created_time > 1577836766 and \$CONDITIONS
-- value higher than 1577836766 belongs to year 2020
group by 1,2,3,4