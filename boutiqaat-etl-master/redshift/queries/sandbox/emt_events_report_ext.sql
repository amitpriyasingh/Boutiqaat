BEGIN;

/*

----------------------------------------ONE TIME MISSING CELEBRITY_ID and EVENT_ID--------------------------------
update aoi.events_report 
set event_id = celebrity_id||'---'||event_portal ||'---'||event_type||'---'||event_date||'---'||to_char(event_time,'HH24:MI:SS:000000') 
where trim(event_id) is null;

update aoi.events_report  
set celebrity_id =9171,
event_id = '9171---'||event_portal ||'---'||event_type||'---'||event_date||'---'||to_char(event_time,'HH24:MI:SS:000000') 
where celebrity_name='il7e9 Boutique' and event_id is null;

update aoi.events_report  
set celebrity_id =199,
event_id = '199---'||event_portal ||'---'||event_type||'---'||event_date||'---'||to_char(event_time,'HH24:MI:SS:000000')  
where celebrity_name='Abeer Jaber Boutique' and event_id is null;


update aoi.events_report  
set celebrity_id =323, 
	event_id = '323'||'---'||event_portal ||'---'||event_type||'---'||event_date||'---'||to_char(event_time,'HH24:MI:SS:000000')
where celebrity_name='Qout Boutique' and event_id is null;

update aoi.events_report  
set celebrity_id =9331, 
	event_id = '9331'||'---'||event_portal ||'---'||event_type||'---'||event_date||'---'||to_char(event_time,'HH24:MI:SS:000000')
where celebrity_name='Bashayer Jumaa Boutique' and event_id is null;

--------------------------------------ONE TIME CODE FOR EMT_EVENTS_REPORT_ONETIME --------------------------------------
drop table sandbox.emt_events_report_onetime ;
create table sandbox.emt_events_report_onetime as 
select id,replace(event_id,split_part(event_id,'---',4),bq_post||'---'||split_part(event_id,'---',4)) as event_id,
events_header_id,user_name,celebrity_name,celebrity_id,generic,event_portal,event_type,event_class,bq_post,total_post,event_date,event_time,created_at,updated_at,event_hours,event_minutes,remark,labelid,productid,category1,category2,category3,category4,brand,purchase_type,account_manager,sku_name,sku_code,sku_code_name,alloc_bq_post,ranking
 from  (
					select er.*,
						row_number() over( partition by event_id, bq_post, event_date,celebrity_id, sku_code, labelid order by event_id desc) as ranking
						from aoi.events_report er
					where generic ='Generic' 
				  ) as events_report where ranking = 1
UNION ALL	
select
	cast(NULL as int) AS id,emt.event_id,emt.events_header_id,emt.user_name,emt.celebrity_name ,emt.celebrity_id ,emt.generic ,emt.event_portal ,emt.event_type ,emt.event_class ,emt.bq_post ,emt.total_post ,emt.event_date ,emt.event_time ,emt.created_at ,emt.updated_at ,emt.event_hours ,emt.event_minutes ,emt.remark ,cast(emt.labelid as varchar) as labelid,cast(emt.product_entity_id as varchar)AS productid,
	NULL AS category1,NULL AS category2 ,NULL AS category3,NULL AS category4,NULL AS brand ,NULL AS purchase_type,NULL AS account_manager,NULL AS sku_name,emt.skuid AS sku_code ,NULL AS sku_code_name ,
	cast(NULL as int) AS alloc_bq_post, 
	ranking
from (select emt.*,
			 row_number() over( partition by event_id,event_date,celebrity_id, skuid, labelid order by event_id desc) as ranking
	   from bidb.emt_report emt 
	   where generic = 'Generic' and event_date > (select max(event_date) from aoi.events_report)
	   -- and event_date < date('2020-07-01')
	 ) as emt 
where ranking = 1 
UNION ALL
select
	cast(NULL as int) AS id,emt.event_id,emt.events_header_id,emt.user_name,emt.celebrity_name ,emt.celebrity_id ,emt.generic ,emt.event_portal ,emt.event_type ,emt.event_class ,emt.bq_post ,emt.total_post ,emt.event_date ,emt.event_time ,emt.created_at ,emt.updated_at ,emt.event_hours ,emt.event_minutes ,emt.remark ,cast(emt.labelid as varchar) as labelid,cast(emt.product_entity_id as varchar)AS productid,
	NULL AS category1,NULL AS category2 ,NULL AS category3,NULL AS category4,NULL AS brand ,NULL AS purchase_type,NULL AS account_manager,NULL AS sku_name,emt.skuid AS sku_code ,NULL AS sku_code_name ,
	cast(NULL as int) AS alloc_bq_post, 
	ranking
from (select emt.*,
			 row_number() over( partition by  event_id,event_date,celebrity_id, skuid, labelid order by event_id desc) as ranking
	   from bidb.emt_report emt 
	   where generic <> 'Generic' 
	   -- and celebrity_id is not null -- Need to remove but ignoring as of now : will Update TABLE
	 ) as emt 
where ranking = 1 
union all 
select id,replace(event_id,split_part(event_id,'---',4),bq_post||'---'||split_part(event_id,'---',4)) as event_id,
events_header_id,user_name,celebrity_name,celebrity_id,generic,event_portal,event_type,event_class,bq_post,total_post,event_date,event_time,created_at,updated_at,event_hours,event_minutes,remark,labelid,productid,category1,category2,category3,category4,brand,purchase_type,account_manager,sku_name,sku_code,sku_code_name,alloc_bq_post,ranking
 from  (
					select er.*,
						row_number() over( partition by event_id, bq_post, event_date,celebrity_id, sku_code, labelid order by event_id desc) as ranking
						from aoi.events_report er
					where generic <> 'Generic' 
					and replace(er.event_id,split_part(er.event_id,'---',4),er.bq_post||'---'||split_part(er.event_id,'---',4)) not in (select event_id from bidb.emt_report where generic <> 'Generic' and event_date < (select max(event_date)+1 from aoi.events_report) )
				  ) as events_report where ranking = 1
;

insert into sandbox.emt_events_report_onetime 
select
	cast(NULL as int) AS id,emt.event_id,emt.events_header_id,emt.user_name,emt.celebrity_name ,emt.celebrity_id ,emt.generic ,emt.event_portal ,emt.event_type ,emt.event_class ,emt.bq_post ,emt.total_post ,emt.event_date ,emt.event_time ,emt.created_at ,emt.updated_at ,emt.event_hours ,emt.event_minutes ,emt.remark ,cast(emt.labelid as varchar) as labelid,cast(emt.product_entity_id as varchar)AS productid,
	NULL AS category1,NULL AS category2 ,NULL AS category3,NULL AS category4,NULL AS brand ,NULL AS purchase_type,NULL AS account_manager,NULL AS sku_name,emt.skuid AS sku_code ,NULL AS sku_code_name ,
	cast(NULL as int) AS alloc_bq_post, 
	ranking
from (select emt.*,
			 row_number() over( partition by event_id,event_date,celebrity_id, skuid, labelid order by event_id desc) as ranking
	   from bidb.emt_report emt 
	   where generic = 'Generic' and event_id not in (select event_id from sandbox.emt_events_report_onetime where generic = 'Generic')
	   -- and event_date < date('2020-07-01')
	 ) as emt 
where ranking = 1 ;

----------------------------------------- MISSING CELEBRITY_ID --------------------------------
update sandbox.emt_events_report_onetime 
set celebrity_id =9171 
where celebrity_name='il7e9 Boutique' and celebrity_id is null;

update sandbox.emt_events_report_onetime 
set celebrity_id =199 
where celebrity_name='Abeer Jaber Boutique' and celebrity_id is null;

update sandbox.emt_events_report_onetime 
set celebrity_id =323 
where celebrity_name='Qout Boutique' and celebrity_id is null;

update sandbox.emt_events_report_onetime 
set celebrity_id =9331 
where celebrity_name='Bashayer Jumaa Boutique' and celebrity_id is null;

---------------------------------- FINAL REPORT: sandbox.emt_events_report_ext --------------------------------
		
drop table sandbox.emt_events_report_ext;		
create table sandbox.emt_events_report_ext as							
select distinct emt.event_id, emt.events_header_id, emt.user_name, 
		coalesce(am.celebrity_name, emt.celebrity_name) as celebrity_name, 
		emt.celebrity_id, 
		emt.generic, emt.event_portal, 
		emt.event_type, emt.event_class,emt.bq_post, emt.total_post, 
		emt.event_date, emt.event_time, emt.created_at, emt.updated_at, 
		emt.event_hours, emt.event_minutes, emt.remark,emt.labelid,emt.productid,
		coalesce(nsm.category1,sm.category1,emt.category1) as category1,
		coalesce(nsm.category2,sm.category2,emt.category2) as category2,
		coalesce(nsm.category3,sm.category3,emt.category3) as category3,
		coalesce(nsm.category4,sm.category4,emt.category4) as category4,
		coalesce(nsm.brand,sm.brand,emt.brand) as brand,
		-- emt.purchase_type ,
		coalesce(am.account_manager,ocm.account_manager,cam.account_manager) as account_manager,
		coalesce(nsm.sku_name,sm.sku_name,emt.sku_name) as sku_name,
		emt.sku_code, emt.sku_code_name,emt.alloc_bq_post ,
		coalesce(am.am_email,cam.am_email) as am_email,
		coalesce(am.rm_email,cam.rm_email) as rm_email,
		coalesce(am.rm_name,cam.rm_name) as rm_name
from sandbox.emt_events_report_onetime emt 
left join (select parent_sku as sku, min(sku_name) as sku_name, min(brand) as brand, min(category1) as category1, 
			min(category2) as category2, min(category3) as category3, min(category4) as category4
			from magento.sku_master where parent_sku<> sku group by 1
		 union
		  select sku as sku, min(sku_name) as sku_name, min(brand) as brand, min(category1) as category1, 
			min(category2) as category2, min(category3) as category3, min(category4) as category4
		from magento.sku_master	group by 1		
) sm on upper(emt.sku_code) = upper(sm.sku) 
left join nav.nav_sku_master nsm on upper(emt.sku_code) = upper(nsm.sku)
left join bidb.historical_celeb_am_mapping ocm on ocm.celebrity_id= emt.celebrity_id and ocm.event_date = emt.event_date
left join sandbox.celebrity_am_mapping am on am.celebrity_id = emt.celebrity_id and emt.event_date between am_start_date and am_end_date
left join (select celebrity_id, account_manager, am_email,rm_email,rm_name
			from (select celebrity_id, account_manager, am_email,rm_email,rm_name,
						 row_number() over(partition by celebrity_id order by am_start_date) as rnk 
				    from sandbox.celebrity_am_mapping
				  )m where rnk= 1
          )cam on cam.celebrity_id=emt.celebrity_id; 
		  
*/	  
-----------------------------------------------INCREMENTAL 30 DAYS -----------------------------------------------

delete from sandbox.emt_events_report_ext where event_date > current_date-30;

insert into sandbox.emt_events_report_ext	
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
		-- emt.purchase_type,
		coalesce(am.account_manager,cam.account_manager) as account_manager,
		coalesce(sm.sku_name,emt.sku_name) as sku_name,
		emt.sku_code, emt.sku_code_name, emt.alloc_bq_post, 
		coalesce(am.am_email,cam.am_email) as am_email,
		coalesce(am.rm_email,cam.rm_email) as rm_email,
		coalesce(am.rm_name,cam.rm_name) as rm_name
from (select
	  cast(NULL as int) AS id,emt.event_id,emt.events_header_id,emt.user_name,emt.celebrity_name ,emt.celebrity_id ,emt.generic ,emt.event_portal ,emt.event_type ,emt.event_class ,emt.bq_post ,emt.total_post ,emt.event_date ,emt.event_time ,emt.created_at ,emt.updated_at ,emt.event_hours ,emt.event_minutes ,emt.remark ,cast(emt.labelid as varchar) as labelid,cast(emt.product_entity_id as varchar)AS productid,
	  NULL AS category1,NULL AS category2 ,NULL AS category3,NULL AS category4,NULL AS brand ,NULL AS purchase_type,NULL AS account_manager,NULL AS sku_name,emt.skuid AS sku_code ,NULL AS sku_code_name ,
	  cast(NULL as int) AS alloc_bq_post, 
	  ranking
from  (select emt.*,
			 row_number() over( partition by  event_id,event_date,celebrity_id, skuid,labelid,Generic order by event_id desc) as ranking
	   from bidb.emt_report emt 
	   where event_date > current_date-30
	   ) emt where ranking = 1 
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
						 row_number() over(partition by celebrity_id order by am_start_date) as rnk 
				    from sandbox.celebrity_am_mapping
				  )m where rnk= 1
          )cam on cam.celebrity_id=emt.celebrity_id;

-----------------------------------------------MUST UPDATE -------------------------------------------

update sandbox.emt_events_report_ext
set    alloc_bq_post = 1/alloc_bq_post_count
from (select event_id, count(1) as alloc_bq_post_count from  sandbox.emt_events_report_ext group by event_id) a
where sandbox.emt_events_report_ext.event_id = a.event_id;

update sandbox.emt_events_report_ext
set   brand = coalesce(emt_events_report_ext.brand,soh.brand), 
	  category1 = coalesce(emt_events_report_ext.category1,soh.category1), 
      category2 = coalesce(emt_events_report_ext.category2,soh.category2),
      category3 = coalesce(emt_events_report_ext.category3,soh.category3),
	  category4 = coalesce(emt_events_report_ext.category4,soh.category4)
from analytics.soh_report soh  
where soh.sku = emt_events_report_ext.sku_code
and (  emt_events_report_ext.brand is null 
	or emt_events_report_ext.category1 is null 
	or emt_events_report_ext.category2 is null 
	or emt_events_report_ext.category3 is null 
	or emt_events_report_ext.category4 is null
   );

COMMIT;

