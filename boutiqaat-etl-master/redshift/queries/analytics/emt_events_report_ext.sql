-- Refrence: boutiqaat-etl/redshift/queries/sandbox/1576_EVENT_REPORT_WORK_HIST_BKP.sql

-- STEP-1: Load all data from Fully Merged table sandbox.emt_events_report_ext till 30 AUG 2020:
/*
create table analytics.celebrity_events_report as 
select f.* from 
(
select m.*, row_number() over( partition by event_id,event_date,celebrity_id, sku_code, coalesce(labelid,'-1'), generic order by event_id desc) as ranking
from sandbox.emt_events_report_ext m 
) f  where ranking = 1;

alter table  analytics.celebrity_events_report 
drop column ranking;
*/

-- STEP-2: INCREMENTAL LOAD in 30 DAYS:

BEGIN;

delete from analytics.celebrity_events_report where event_date > current_date-30;

insert into analytics.celebrity_events_report	
select distinct emt.event_id, emt.events_header_id, emt.user_name, 
		emt.celebrity_name, emt.celebrity_id, emt.generic, emt.event_portal, 
		emt.event_type, emt.event_class,emt.bq_post, emt.total_post, 
		emt.event_date, emt.event_time, emt.created_at, emt.updated_at, 
		emt.event_hours, emt.event_minutes, emt.remark,emt.labelid,emt.productid,
		coalesce(nsm.category1,sm.category1,emt.category1) as category1,
		coalesce(nsm.category2,sm.category2,emt.category2) as category2,
		coalesce(nsm.category3,sm.category3,emt.category3) as category3,
		coalesce(nsm.category4,sm.category4,emt.category4) as category4,
		coalesce(nsm.brand,sm.brand,emt.brand) as brand,
		-- emt.purchase_type ,
		coalesce(am.account_manager,cam.account_manager) as account_manager,
		coalesce(sm.sku_name,emt.sku_name) as sku_name,
		emt.sku_code, emt.sku_code_name, emt.alloc_bq_post, 
		coalesce(am.am_email,cam.am_email) as am_email,
		coalesce(am.rm_email,cam.rm_email) as rm_email,
		coalesce(am.rm_name,cam.rm_name) as rm_name
from (select
	        emt.event_id,emt.events_header_id,emt.user_name,emt.celebrity_name ,emt.celebrity_id ,emt.generic ,emt.event_portal ,emt.event_type ,emt.event_class ,emt.bq_post ,emt.total_post ,emt.event_date ,emt.event_time ,emt.created_at ,emt.updated_at ,emt.event_hours ,emt.event_minutes ,emt.remark ,cast(emt.labelid as varchar) as labelid,cast(emt.product_entity_id as varchar)AS productid,
	        NULL AS category1,NULL AS category2 ,NULL AS category3,NULL AS category4,NULL AS brand ,NULL AS purchase_type,NULL AS account_manager,NULL AS sku_name,emt.skuid AS sku_code ,NULL AS sku_code_name, cast(NULL as int) AS alloc_bq_post
	   from bidb.emt_report emt where event_date > current_date-30 -- NO DUPLICATES
	  ) emt
left join (select parent_sku as sku, min(sku_name) as sku_name, min(brand) as brand, min(category1) as category1, 
			min(category2) as category2, min(category3) as category3, min(category4) as category4
			from magento.sku_master where parent_sku<> sku group by 1
		 union
		  select sku as sku, min(sku_name) as sku_name, min(brand) as brand, min(category1) as category1, 
			min(category2) as category2, min(category3) as category3, min(category4) as category4
		from magento.sku_master	group by 1		
) sm on upper(emt.sku_code) = upper(sm.sku) 
left join nav.nav_sku_master nsm on upper(emt.sku_code) = upper(nsm.sku)
left join sandbox.celebrity_am_mapping am on am.celebrity_id = emt.celebrity_id and emt.event_date between am_start_date and am_end_date	
left join (select celebrity_id, account_manager, am_email,rm_email,rm_name
			from (select celebrity_id, account_manager, am_email,rm_email,rm_name,
						 row_number() over(partition by celebrity_id order by am_start_date desc) as rnk 
				    from sandbox.celebrity_am_mapping
				  )m where rnk= 1
          )cam on cam.celebrity_id=emt.celebrity_id;

-- STEP-3-------------------------------------MUST UPDATE -------------------------------------------

update analytics.celebrity_events_report
set    alloc_bq_post = 1/alloc_bq_post_count
from (select event_id, count(1) as alloc_bq_post_count from  analytics.celebrity_events_report group by event_id) a
where analytics.celebrity_events_report.event_id = a.event_id;


update analytics.celebrity_events_report
set   brand = coalesce(celebrity_events_report.brand,soh.brand), 
	  category1 = coalesce(celebrity_events_report.category1,soh.category1), 
      category2 = coalesce(celebrity_events_report.category2,soh.category2),
      category3 = coalesce(celebrity_events_report.category3,soh.category3),
	  category4 = coalesce(celebrity_events_report.category4,soh.category4)
from analytics.soh_report soh  
where soh.sku = celebrity_events_report.sku_code
and (  celebrity_events_report.brand is null 
	or celebrity_events_report.category1 is null 
	or celebrity_events_report.category2 is null 
	or celebrity_events_report.category3 is null 
	or celebrity_events_report.category4 is null
   );
   
COMMIT;
  
-----------------------------------END ----------------------------------
