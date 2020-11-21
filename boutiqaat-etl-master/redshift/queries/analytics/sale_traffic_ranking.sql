BEGIN;

-- Step1: Celebrity_Sale
truncate table sandbox.celebrity_sale;
insert into sandbox.celebrity_sale
SELECT 
celebrity_id, min(celebrity_name) as celebrity_name,  coalesce(SUM(net_sale_price_kwd),0)  as celebrity_revenue,
row_number() over(order by coalesce(SUM(net_sale_price_kwd),0) asc) as celebrity_rank
FROM aoi.order_details
where order_date >= current_date-28
and order_category<>'CELEBRITY'
group by 1;

truncate table sandbox.celebrity_sale_traffic_rank;
insert into sandbox.celebrity_sale_traffic_rank 
select f.*, 
	   row_number() over(order by 1.000*(coalesce(sale_rank,0)+ coalesce(traffic_rank,0))/2 asc) as sorting_rank
from (
select celebrity_id, celebrity_name, 
	   celebrity_revenue as sale, 
	   coalesce(total_uniq_users,0) as traffic,
	   sale_conversion,
	   avg_conversion,
	   sale_rank, 
	   row_number() over(order by coalesce(coalesce(celebrity_revenue,0)/total_uniq_users,avg_conversion) asc) as traffic_rank
from(
	select coalesce(celebrity_id,page_id) as celebrity_id, 
		   coalesce(celebrity_name,page_name) as celebrity_name, 
		   coalesce(celebrity_revenue,0) as celebrity_revenue, 	          
	       NULLIF(total_uniq_users,0) as total_uniq_users,
		   coalesce((coalesce(celebrity_revenue,0)/NULLIF(total_uniq_users,0)),0) as sale_conversion,
	       (sum(celebrity_revenue) over())/(sum(total_uniq_users) over()) as avg_conversion,
	       row_number() over(order by coalesce(celebrity_revenue,0) asc) as sale_rank		   
	from sandbox.celebrity_sale a
    full join 
		(
		 select cast((case when page_id ='na' then NULL else page_id end) as integer) page_id, 
			    min(page_name) page_name , 
				sum(total_uniq_users) as total_uniq_users 
		   from firebase.daily_traffic 
		  where page_type ='Celebrity' and event_date_kwt  >= current_date-28
		    and page_id <> 'na' and page_id is not null
		    group by 1
		) b 
		on a.celebrity_id = page_id where coalesce(celebrity_id,page_id) is not null 
) a ) f ;

-- STEP2 brand_sale
truncate table sandbox.brand_sale ;
insert into sandbox.brand_sale 
SELECT 
 b.brand_id, coalesce(brand_name,brand) brand, coalesce(SUM(net_sale_price_kwd),0) as brand_revenue,
row_number() over(order by coalesce(SUM(net_sale_price_kwd),0) asc) brand_rank
from 
(
	select sku, min(brand) as brand, coalesce(SUM(net_sale_price_kwd),0) as net_sale_price_kwd
	FROM aoi.order_details
	where order_date >= current_date -28
	and order_category<>'CELEBRITY' 
	group by 1
) s
right join magento.sku_brands b on s.sku = b.sku 
group by 1, 2;

truncate table sandbox.brand_sale_traffic_rank;
insert into sandbox.brand_sale_traffic_rank
select f.*, 
	   row_number() over(order by 1.000*(coalesce(sale_rank,0)+ coalesce(traffic_rank,0))/2 asc) as sorting_rank
from (
select a.brand_id, a.brand_name,
	   brand_revenue as sale, 
	   coalesce(total_uniq_users,0) as traffic,
	   sale_conversion,
	   avg_conversion,
	   sale_rank, 
	   row_number() over(order by coalesce( coalesce(brand_revenue,0)/total_uniq_users, avg_conversion) asc) as traffic_rank
from(
	select coalesce(brand_id,page_id) as brand_id, 
		   coalesce(brand_name,page_name) as brand_name, 
		   coalesce(brand_revenue,0) as brand_revenue, 	          
	       NULLIF(total_uniq_users,0) as total_uniq_users,
		   coalesce((coalesce(brand_revenue,0)/NULLIF(total_uniq_users,0)),0) as sale_conversion,
	       (sum(brand_revenue) over())/(sum(total_uniq_users) over()) as avg_conversion,
	       row_number() over(order by coalesce(brand_revenue,0) asc) as sale_rank		   
	from sandbox.brand_sale a
    full outer join 
		(
		 select cast((case when page_id ='na' then NULL else page_id end) as integer) page_id, 
			    min(page_name) page_name , 
				sum(total_uniq_users) as total_uniq_users 
		   from firebase.daily_traffic 
		   join (select distinct brand_id from magento.sku_brands) on brand_id = cast((case when page_id ='na' then NULL else page_id end) as integer)
		  where page_type ='Brand' and event_date_kwt  >= current_date-28
		    and page_id <> 'na' and page_id is not null
		    group by 1
		) b 
		on a.brand_id = page_id where coalesce(brand_id,page_id) is not null
	) a 
) f ;


-- STEP3 category_sale: 
truncate table sandbox.category_sale ;
insert into sandbox.category_sale
SELECT 
	b.category_id, min(coalesce(b.category_name,s.category_name)) as category_name, 
	coalesce(SUM(net_sale_price_kwd),0) as category_revenue,
row_number() over(order by coalesce(SUM(net_sale_price_kwd),0) asc) category_rank
from 
(
	select sku, min(category1) as category_name, coalesce(SUM(net_sale_price_kwd),0) as net_sale_price_kwd
	FROM aoi.order_details
	where order_date >= current_date -28
	and order_category<>'CELEBRITY' 
	group by 1
) s
right join
(
	select child_sku as sku, category_id, category_name from magento.sku_categories
) b on s.sku = b.sku
group by 1;

truncate table sandbox.category_sale_traffic_rank;
insert into sandbox.category_sale_traffic_rank  
select f.*, 
	   row_number() over(order by 1.000*(coalesce(sale_rank,0)+ coalesce(traffic_rank,0))/2 asc) as sorting_rank
from (
select category_id, category_name, 
	   category_revenue as sale, 
	   coalesce(total_uniq_users,0) as traffic,
	   sale_conversion,
	   avg_conversion,
	   sale_rank, 
	   row_number() over(order by coalesce(coalesce(category_revenue,0)/total_uniq_users,avg_conversion) asc) as traffic_rank
from(
	select coalesce(category_id,page_id) as category_id, 
		   coalesce(category_name,page_name) as category_name, 
		   coalesce(category_revenue,0) as category_revenue, 	          
	       NULLIF(total_uniq_users,0) as total_uniq_users,
		   coalesce((coalesce(category_revenue,0)/NULLIF(total_uniq_users,0)),0) as sale_conversion,
	       (sum(category_revenue) over())/(sum(total_uniq_users) over()) as avg_conversion,
	       row_number() over(order by coalesce(category_revenue,0) asc) as sale_rank		   
	from sandbox.category_sale a
    full join 
		(
		 select cast((case when page_id ='na' then NULL else page_id end) as integer) page_id, 
			    min(page_name) page_name , 
				sum(total_uniq_users) as total_uniq_users 
		   from firebase.daily_traffic 
		   join (select distinct category_id from magento.sku_categories) on category_id = cast((case when page_id ='na' then NULL else page_id end) as integer)
		  where page_type ='Category' and event_date_kwt  >= current_date-28
		    and page_id <> 'na' and page_id is not null
		    group by 1
		) b 
		on a.category_id = page_id where coalesce(category_id,page_id) is not null
) a ) f ;

commit;
 
-- sandbox.celebrity_sale_traffic_rank
-- sandbox.category_sale_traffic_rank
-- sandbox.brand_sale_traffic_rank
-- sandbox.celebrity_sale
-- sandbox.category_sale
-- sandbox.brand_sale
-- sandbox.dgsorting_score_vw
-- sandbox.celebrity_traffic_vw
-- sandbox.category_traffic_vw
-- sandbox.brand_traffic_vw 

-- drop view sandbox.celebrity_traffic_vw;
-- drop view sandbox.celebrity_traffic_vw;
-- drop view sandbox.category_traffic_vw;
-- drop view sandbox.brand_traffic_vw;
-- create view sandbox.celebrity_traffic_vw as select celebrity_id, sorting_rank from sandbox.celebrity_sale_traffic_rank;
-- create view sandbox.category_traffic_vw as select category_id, sorting_rank from sandbox.category_sale_traffic_rank;
-- create view sandbox.brand_traffic_vw as select brand_id, sorting_rank from sandbox.brand_sale_traffic_rank;
