BEGIN;
insert into analytics.sku_online_status(sku,report_date,updated_at)
select sku, convert_timezone('Asia/Kuwait',GETDATE())::date, convert_timezone('Asia/Kuwait',GETDATE()) 
from analytics.sku_online_status 
where (select count(1) from analytics.sku_online_status  where report_date=convert_timezone('Asia/Kuwait',GETDATE())::date)=0 
group by sku;
insert into analytics.sku_online_status(sku,report_date,updated_at)
select sku, convert_timezone('Asia/Kuwait',GETDATE())::date, convert_timezone('Asia/Kuwait',GETDATE()) 
from sandbox.sku_online_status_staging 
where sku not in (select sku from analytics.sku_online_status where report_date=convert_timezone('Asia/Kuwait',GETDATE())::date)
group by sku;
update analytics.sku_online_status master 
set online = case when merged.online is null then master.online else COALESCE(master.online,0) + merged.online end,
in_stock = case when merged.in_stock is null then master.in_stock else COALESCE(master.in_stock,0) + merged.in_stock end,
enabled = case when merged.enabled is null then master.enabled else COALESCE(master.enabled,0) + merged.enabled end, 
updated_at = convert_timezone('Asia/Kuwait',GETDATE())
from
(select master.report_date::date, master.sku, staging.enabled::int, staging.in_stock::int, staging.online::int 
from analytics.sku_online_status master left join sandbox.sku_online_status_staging staging on staging.sku=master.sku 
and staging.report_date::date=master.report_date::date) merged
where merged.sku=master.sku and merged.report_date=master.report_date;
COMMIT;