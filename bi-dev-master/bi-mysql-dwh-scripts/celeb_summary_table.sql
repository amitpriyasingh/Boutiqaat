insert into bi_celebrity_master(celebrity_name,celebrity_id,boarding_date,created_at, source_table) select celebrity_name,celebrity_id, date(created_at), now(), 'celebrity_master' from celebrity_master where celebrity_id not in (select celebrity_id from bi_celebrity_master) group by celebrity_id;

insert into bi_celebrity_master(celebrity_name,celebrity_id,boarding_date,created_at, source_table) select celebrity_name,celebrity_id, date(MIN(order_date)), now(), 'aoi' from aoi where celebrity_id not in (select celebrity_id from bi_celebrity_master) group by celebrity_id;

insert into bi_celebrity_master(celebrity_name,celebrity_id,boarding_date,created_at, source_table) select celebrity_name,celebrity_id, date(MIN(event_date)), now(), 'events_header' from events.events_header where celebrity_id not in (select celebrity_id from bi_celebrity_master) group by celebrity_id;

update bi_celebrity_master bcm, (select celeb_emt.* from (select distinct celebrity_id,celebrity_name from events.events_header) celeb_emt left join (select distinct celebrity_id,celebrity_name from bi_celebrity_master) celeb_bicm on celeb_bicm.celebrity_id=celeb_emt.celebrity_id where celeb_emt.celebrity_name IS NOT NULL AND celeb_bicm.celebrity_name IS NULL) celeb_emt set bcm.celebrity_name=celeb_emt.celebrity_name
where bcm.celebrity_id=celeb_emt.celebrity_id;

update bi_celebrity_master bcm left join (select cmap.CELEBID celebrity_id, cord.NAME_E account_manager from (select * from CELEBRITY_GROUP where GROUPID <> 13 and CELEBID <> 94 GROUP BY CELEBID) cmap left join CELEB_GROUPS cord on cord.ID = cmap.GROUPID group by cmap.CELEBID) cam on bcm.celebrity_id=cam.celebrity_id
set bcm.current_am = cam.account_manager
WHERE cam.account_manager IS NOT NULL;

delete from celeb_summary_monthly_metrics where report_month = Extract(YEAR_MONTH FROM CURRENT_DATE);

INSERT INTO celeb_summary_monthly_metrics(report_month ,celebrity_id ,celebrity_name ,account_manager ,unique_sku ,total_orders ,total_qty ,total_rev ,total_margin ,gift_orders ,gift_cogs ,total_events ,total_post ,visitors ,primary_sessions ,sec_sessions ,total_sessions ,orders_per_session ,rev_per_session ,rev_per_events ,qty_per_events ,boarding_date ,DAY_FIRST ,DAY_LAST ,rev_per_day ,qty_per_day ,elapsed_days ,projection_days, elapsed_days_weighted_percent, projection_days_weighted_percent,normalized_rev_per_day,normalized_qty_per_day) 
select f.*, rev_per_day, qty_per_day from (select e.*, (total_rev / (DATEDIFF(IF(DATEDIFF(CURRENT_DATE()-INTERVAL 1 DAY,DAY_LAST)<0,CURRENT_DATE()-INTERVAL 1 DAY,DAY_LAST), IF(boarding_date > DAY_FIRST,boarding_date,DAY_FIRST))+1)) rev_per_day, (total_qty / (DATEDIFF(IF(DATEDIFF(CURRENT_DATE()-INTERVAL 1 DAY,DAY_LAST)<0,CURRENT_DATE()-INTERVAL 1 DAY,DAY_LAST), IF(boarding_date > DAY_FIRST,boarding_date,DAY_FIRST))+1)) qty_per_day, (DATEDIFF(IF(DATEDIFF(CURRENT_DATE()-INTERVAL 1 DAY,DAY_LAST)<0,CURRENT_DATE()-INTERVAL 1 DAY,DAY_LAST), IF(boarding_date > DAY_FIRST,boarding_date,DAY_FIRST))+1) elapsed_days, DATEDIFF(DAY_LAST, IF(boarding_date > DAY_FIRST,boarding_date,DAY_FIRST)) + 1 projection_days,
(select sum(revenue_share) from revenue_share where day_of_month <= DAY(IF(DATEDIFF(CURRENT_DATE()-INTERVAL 1 DAY,DAY_LAST)<0,CURRENT_DATE()-INTERVAL 1 DAY,DAY_LAST))) elapsed_days_weighted_percent,
(select sum(revenue_share) from revenue_share where day_of_month <= DAY(DAY_LAST)) projection_days_weighted_percent
FROM (SELECT Extract(YEAR_MONTH FROM `date`) report_month, ca.celebrity_id, IFNULL(ca.celebrity_name,bcm.celebrity_name), IFNULL(ca.account_manager,bcm.current_am), Sum(unique_sku) unique_sku, Sum(total_orders) total_orders, Sum(total_qty) total_qty, Sum(total_rev) total_rev, Sum(total_margin) total_margin, Sum(gift_orders) gift_orders, Sum(gift_cogs) gift_cogs, Sum(total_events) total_events, Sum(total_post) total_post, Sum(users) visitors, Sum(primary_sessions) primary_sessions, Sum(sec_sessions) sec_sessions, Sum(primary_sessions + sec_sessions) total_sessions, Sum(total_orders) / Sum(primary_sessions + sec_sessions) orders_per_session, Sum(total_rev) / Sum(primary_sessions + sec_sessions) rev_per_session, Sum(total_rev) / Sum(total_events) rev_per_events, Sum(total_qty) / Sum(total_events) qty_per_events, IFNULL(bcm.boarding_date,CURRENT_DATE() - INTERVAL 1 DAY) boarding_date, DATE_ADD(DATE_ADD(LAST_DAY(`date`),INTERVAL 1 DAY),INTERVAL - 1 MONTH) DAY_FIRST, LAST_DAY(`date`) DAY_LAST FROM celeb_agg ca left join bi_celebrity_master bcm ON ca.celebrity_id=bcm.celebrity_id where month = Extract(YEAR_MONTH FROM CURRENT_DATE) GROUP BY month, ca.celebrity_id)e) f;

update celeb_summary_monthly_metrics csmm
LEFT JOIN celeb_summary_monthly_metrics pm ON csmm.celebrity_id=pm.celebrity_id and pm.DAY_LAST = LAST_DAY(csmm.DAY_LAST - INTERVAL 31 DAY)
set csmm.normalized_rev_per_day=IF(pm.normalized_rev_per_day IS NULL OR pm.special_sale_month=1, 0, pm.normalized_rev_per_day), csmm.normalized_qty_per_day=IF(pm.normalized_qty_per_day IS NULL OR pm.special_sale_month=1, 0, pm.normalized_qty_per_day)
WHERE csmm.special_sale_month=1;

delete from celeb_summary where report_month = Extract(YEAR_MONTH FROM CURRENT_DATE);

INSERT INTO celeb_summary SELECT
s1.report_month,
s1.DAY_FIRST, s1.DAY_LAST,
s1.celebrity_id, 
s1.celebrity_name, 
s1.account_manager, 
s1.unique_sku, 
s1.total_orders, 
s1.total_qty, 
s1.total_rev, 
s1.total_margin, 
s1.gift_orders, 
s1.gift_cogs, 
s1.total_events, 
s1.total_post, 
s1.visitors, 
s1.primary_sessions, 
s1.sec_sessions, 
s1.total_sessions, 
s1.orders_per_session, 
s1.rev_per_session, 
s1.rev_per_events, 
s1.rev_per_day, 
s1.qty_per_events, 
s1.qty_per_day,
(s1.unique_sku/s1.elapsed_days_weighted_percent)*s1.projection_days_weighted_percent proj_unique_sku, 
(s1.total_orders/s1.elapsed_days_weighted_percent)*s1.projection_days_weighted_percent proj_total_orders, 
(s1.total_qty/s1.elapsed_days_weighted_percent)*s1.projection_days_weighted_percent proj_total_qty, 
(s1.total_rev/s1.elapsed_days_weighted_percent)*s1.projection_days_weighted_percent proj_total_rev, 
(s1.total_margin/s1.elapsed_days_weighted_percent)*s1.projection_days_weighted_percent proj_total_margin, 
(s1.gift_orders/s1.elapsed_days_weighted_percent)*s1.projection_days_weighted_percent proj_gift_orders, 
(s1.gift_cogs/s1.elapsed_days_weighted_percent)*s1.projection_days_weighted_percent proj_gift_cogs, 
(s1.total_events/s1.elapsed_days_weighted_percent)*s1.projection_days_weighted_percent proj_total_events, 
(s1.total_post/s1.elapsed_days_weighted_percent)*s1.projection_days_weighted_percent proj_total_post, 
(s1.visitors/s1.elapsed_days_weighted_percent)*s1.projection_days_weighted_percent proj_visitors, 
(s1.primary_sessions/s1.elapsed_days_weighted_percent)*s1.projection_days_weighted_percent proj_primary_sessions, 
(s1.sec_sessions/s1.elapsed_days_weighted_percent)*s1.projection_days_weighted_percent proj_sec_sessions,

IF(s2.projection_days IS NULL,0,(SUM(s2.unique_sku)/SUM(s2.projection_days))*DAY(s1.DAY_LAST)) target_unique_sku,
IF(s2.projection_days IS NULL,0,(SUM(s2.total_orders)/SUM(s2.projection_days))*DAY(s1.DAY_LAST)) target_total_orders,

MAX(s2.normalized_qty_per_day)*DAY(s1.DAY_LAST)*1.05 target_total_qty,
MAX(s2.normalized_rev_per_day)*DAY(s1.DAY_LAST)*1.05 target_total_rev,

IF(s2.projection_days IS NULL,0,(SUM(s2.total_margin)/SUM(s2.projection_days))*DAY(s1.DAY_LAST)) target_total_margin,
IF(s2.projection_days IS NULL,0,(SUM(s2.gift_orders)/SUM(s2.projection_days))*DAY(s1.DAY_LAST)) target_gift_orders,
IF(s2.projection_days IS NULL,0,(SUM(s2.gift_cogs)/SUM(s2.projection_days))*DAY(s1.DAY_LAST)) target_gift_cogs,
IF(s2.projection_days IS NULL,0,(SUM(s2.total_events)/SUM(s2.projection_days))*DAY(s1.DAY_LAST)) target_total_events,
IF(s2.projection_days IS NULL,0,(SUM(s2.total_post)/SUM(s2.projection_days))*DAY(s1.DAY_LAST)) target_total_post,
IF(s2.projection_days IS NULL,0,(SUM(s2.visitors)/SUM(s2.projection_days))*DAY(s1.DAY_LAST)) target_visitors,
IF(s2.projection_days IS NULL,0,(SUM(s2.primary_sessions)/SUM(s2.projection_days))*DAY(s1.DAY_LAST)) target_primary_sessions,
IF(s2.projection_days IS NULL,0,(SUM(s2.sec_sessions)/SUM(s2.projection_days))*DAY(s1.DAY_LAST)) target_sec_sessions,
IF(s2.projection_days IS NULL,0,(SUM(s2.total_sessions)/SUM(s2.projection_days))*DAY(s1.DAY_LAST)) target_total_sessions,
IF(s2.projection_days IS NULL,0,(SUM(s2.orders_per_session)/SUM(s2.projection_days))*DAY(s1.DAY_LAST)) target_orders_per_session,
IF(s2.projection_days IS NULL,0,(SUM(s2.rev_per_session)/SUM(s2.projection_days))*DAY(s1.DAY_LAST)) target_rev_per_session,
IF(s2.projection_days IS NULL,0,(SUM(s2.rev_per_events)/SUM(s2.projection_days))*DAY(s1.DAY_LAST)) target_rev_per_events,
IF(s2.projection_days IS NULL,0,(SUM(s2.rev_per_day)/SUM(s2.projection_days))*DAY(s1.DAY_LAST)) target_rev_per_day,
IF(s2.projection_days IS NULL,0,(SUM(s2.qty_per_events)/SUM(s2.projection_days))*DAY(s1.DAY_LAST)) target_qty_per_events,
IF(s2.projection_days IS NULL,0,(SUM(s2.qty_per_day)/SUM(s2.projection_days))*DAY(s1.DAY_LAST)) target_qty_per_day,
EXTRACT(YEAR_MONTH FROM s1.DAY_LAST - INTERVAL 3 month) START_TARGET_MONTH, 
EXTRACT(YEAR_MONTH FROM s1.DAY_LAST - INTERVAL 1 month) END_TARGET_MONTH, 
IF(s2.projection_days IS NULL,0,SUM(s2.projection_days)) target_days, s1.projection_days projection_days, 
s1.elapsed_days elapsed_days, DAY(s1.DAY_LAST) total_days, s1.boarding_date 
from celeb_summary_monthly_metrics s1 left join celeb_summary_monthly_metrics s2 ON s1.celebrity_id=s2.celebrity_id and s2.report_month between EXTRACT(YEAR_MONTH FROM s1.DAY_LAST - INTERVAL 3 month) AND EXTRACT(YEAR_MONTH FROM s1.DAY_LAST - INTERVAL 1 month)  WHERE s1.report_month = Extract(YEAR_MONTH FROM CURRENT_DATE) group by report_month,celebrity_id;

update celeb_summary
left join (select * from CELEBRITY_GROUP where GROUPID <> 13) cmap on 
cmap.CELEBID = celeb_summary.celebrity_id left join CELEB_GROUPS cord on cord.ID = cmap.GROUPID
SET celeb_summary.account_manager=cord.NAME_E Where celeb_summary.account_manager IS NULL;

delete from events_report_am where event_date >= DATE_FORMAT(NOW() ,'%Y-%m-01');

insert into events_report_am (id, event_id, celebrity_name, celebrity_id, generic, event_portal, event_type, total_post, alloc_bq_post, event_date, created_at, category1, brand, account_manager, sku_code_name, am_email, rm_email, rm_name, monthly_celeb_mtd_total_rev, monthly_celeb_proj_total_rev, monthly_celeb_target_total_rev) select er.id, er.event_id, IFNULL(er.celebrity_name,IFNULL(cs.celebrity_name, bicm.celebrity_name)),  bicm.celebrity_id, er.generic, er.event_portal, er.event_type, er.total_post, er.alloc_bq_post, IFNULL(er.event_date,IFNULL(cs.report_start_date, DATE_FORMAT(NOW() ,'%Y-%m-01'))), er.created_at, er.category1, er.brand, IFNULL(er.account_manager,IFNULL(cs.account_manager,bicm.current_am)), er.sku_code_name, ae.email, ae.reporting_manager_email, ae.reporting_manager, cs.total_rev, cs.proj_total_rev, cs.target_total_rev
from
bi_celebrity_master bicm left join  
	(select celebrity_id, report_month, report_start_date, celebrity_name, account_manager, total_rev, proj_total_rev, target_total_rev from celeb_summary where report_month = EXTRACT(year_month from curdate())) cs  ON bicm.celebrity_id=cs.celebrity_id

	left join 
	(select * from events_report where event_date >= DATE_FORMAT(NOW() ,'%Y-%m-01')) er 
	on bicm.celebrity_id=er.celebrity_id and cs.report_month=extract(year_month from er.event_date)

	left join am_email ae on bicm.current_am=ae.name
WHERE bicm.current_am IS NOT NULL;

update events_report_am era left join 
(select report_month, account_manager, SUM(IFNULL(total_rev,0)) am_total_rev, SUM(IFNULL(proj_total_rev,0)) am_proj_total_rev, SUM(IFNULL(target_total_rev,0)) am_target_total_rev from celeb_summary where target_total_rev > 0 AND report_month = EXTRACT(year_month from curdate()) group by report_month, account_manager) acs
on era.account_manager=acs.account_manager and extract(year_month from era.event_date)=acs.report_month
set era.monthly_am_mtd_total_rev= acs.am_total_rev, era.monthly_am_proj_total_rev = acs.am_proj_total_rev, era.monthly_am_target_total_rev = acs.am_target_total_rev
where era.event_date >= DATE_FORMAT(NOW() ,'%Y-%m-01');

