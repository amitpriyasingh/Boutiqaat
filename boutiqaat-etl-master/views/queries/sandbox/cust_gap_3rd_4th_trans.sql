CREATE OR REPLACE VIEW sandbox.cust_gap_3rd_4th_trans as 
select * from 
(select gap_btw_3rd_4th_trans as days_btw_3rd_4th_trans,customers,
sum(customers) over (order by gap_btw_3rd_4th_trans rows unbounded preceding) as cum_customers,
total_cust,
round(sum(customers) over (order by gap_btw_3rd_4th_trans rows unbounded preceding)::decimal/total_cust,2) as cum_dist from 
(
select gap_btw_3rd_4th_trans, count(distinct phone_no) as customers, total_cust
from 
(
select phone_no,gap_btw_3rd_4th_trans,count(phone_no) over() as total_cust from sandbox.customer_retention
where total_orders_till_date>3
) as t1
where gap_btw_3rd_4th_trans is not null group by gap_btw_3rd_4th_trans,total_cust order by gap_btw_3rd_4th_trans asc
) as t2 ) as t3 
where  (mod(days_btw_3rd_4th_trans,5)=0 and days_btw_3rd_4th_trans between 10 and 100)  
or days_btw_3rd_4th_trans between 0 and 10 or (mod(days_btw_3rd_4th_trans,10)=0 and days_btw_3rd_4th_trans between 10 and 200)
or (mod(days_btw_3rd_4th_trans,100)=0 and days_btw_3rd_4th_trans >200)
WITH NO SCHEMA BINDING;