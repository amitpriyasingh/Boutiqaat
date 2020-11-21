BEGIN;

DROP TABLE IF EXISTS analytics.brand_performance;

SELECT * INTO analytics.brand_performance
FROM
(
SELECT  t1.brand, t1.brand_code, t1.vendor_code, t1.supplier,  t1.category_manager, t1.hod, listagg( DISTINCT t1.department) as department,
count(t1.sku) sku_count, 
min(COALESCE(t1.first_grn_date)) first_grn_date,
sum(COALESCE(t1.soh,0)) soh,
sum(COALESCE(t1.open_po_total_qty,0)) open_po_total_qty,
min(COALESCE(t2.first_sale_date)) first_sale_date,
max(COALESCE(t2.last_sale_date)) last_sale_date, 
sum(COALESCE(t2.sku_count_2017, 0)) sku_count_2017,
sum(COALESCE(t2.sku_count_2018, 0)) sku_count_2018,
sum(COALESCE(t2.sku_count_2019, 0)) sku_count_2019,
sum(COALESCE(t2.sku_count_2020, 0)) sku_count_2020,
sum(COALESCE(t2.Qty_sold_2017,0)) Qty_sold_2017, 
sum(COALESCE(t2.Qty_sold_2018,0)) Qty_sold_2018, 
sum(COALESCE(t2.Qty_sold_2019,0)) Qty_sold_2019,
sum(COALESCE(t2.Qty_sold_2020,0)) Qty_sold_2020,
sum(COALESCE(t2.Qty_sold_180_days,0)) Qty_sold_180_days, 
sum(COALESCE(t2.Qty_sold_90_days,0)) Qty_sold_90_days,
sum(COALESCE(t2.Qty_sold_60_days,0)) Qty_sold_60_days, 
sum(COALESCE(t2.Qty_sold_30_days,0)) Qty_sold_30_days, 
sum(COALESCE(t2.Revenue_2017,0)) Revenue_2017, 
sum(COALESCE(t2.Revenue_2018,0)) Revenue_2018, 
sum(COALESCE(t2.Revenue_2019,0)) Revenue_2019, 
sum(COALESCE(t2.Revenue_2020,0)) Revenue_2020, 
sum(COALESCE(t2.Revenue_180_days,0)) Revenue_180_days, 
sum(COALESCE(t2.Revenue_90_days,0)) Revenue_90_days, 
sum(COALESCE(t2.Revenue_60_days,0)) Revenue_60_days, 
sum(COALESCE(t2.Revenue_30_days,0)) Revenue_30_days, 
sum(COALESCE(t2.Qty_Cancel_2017,0)) Qty_Cancel_2017,
sum(COALESCE(t2.Qty_Cancel_2018,0)) Qty_Cancel_2018, 
sum(COALESCE(t2.Qty_Cancel_2019,0)) Qty_Cancel_2019,
sum(COALESCE(t2.Qty_Cancel_2020,0)) Qty_Cancel_2020, 
sum(COALESCE(t2.Qty_Cancel_180_days,0)) Qty_Cancel_180_days, 
sum(COALESCE(t2.Qty_Cancel_90_days,0)) Qty_Cancel_90_days,
sum(COALESCE(t2.Qty_Cancel_60_days,0)) Qty_Cancel_60_days, 
sum(COALESCE(t2.Qty_Cancel_30_days,0)) Qty_Cancel_30_days, 
sum(COALESCE(t2.Revenue_Cancel_2017,0)) Revenue_Cancel_2017, 
sum(COALESCE(t2.Revenue_Cancel_2018,0)) Revenue_Cancel_2018, 
sum(COALESCE(t2.Revenue_Cancel_2019,0)) Revenue_Cancel_2019,
sum(COALESCE(t2.Revenue_Cancel_2020,0)) Revenue_Cancel_2020, 
sum(COALESCE(t2.Revenue_Cancel_180_days,0)) Revenue_Cancel_180_days, 
sum(COALESCE(t2.Revenue_Cancel_90_days,0)) Revenue_Cancel_90_days,
sum(COALESCE(t2.Revenue_Cancel_60_days,0)) Revenue_Cancel_60_days, 
sum(COALESCE(t2.Revenue_Cancel_30_days,0)) Revenue_Cancel_30_days, 
sum(COALESCE(t2.Qty_Return_2017,0)) Qty_Return_2017, 
sum(COALESCE(t2.Qty_Return_2018,0)) Qty_Return_2018, 
sum(COALESCE(t2.Qty_Return_2019,0)) Qty_Return_2019,
sum(COALESCE(t2.Qty_Return_2020,0)) Qty_Return_2020, 
sum(COALESCE(t2.Qty_Return_180_days,0)) Qty_Return_180_days, 
sum(COALESCE(t2.Qty_Return_90_days,0)) Qty_Return_90_days,
sum(COALESCE(t2.Qty_Return_60_days,0)) Qty_Return_60_days, 
sum(COALESCE(t2.Qty_Return_30_days,0)) Qty_Return_30_days, 
sum(COALESCE(t2.Revenue_Return_2017,0)) Revenue_Return_2017,
sum(COALESCE(t2.Revenue_Return_2018,0)) Revenue_Return_2018, 
sum(COALESCE(t2.Revenue_Return_2019,0)) Revenue_Return_2019,
sum(COALESCE(t2.Revenue_Return_2020,0)) Revenue_Return_2020,
sum(COALESCE(t2.Revenue_Return_180_days,0)) Revenue_Return_180_days, 
sum(COALESCE(t2.Revenue_Return_90_days,0)) Revenue_Return_90_days,
sum(COALESCE(t2.Revenue_Return_60_days,0)) Revenue_Return_60_days, 
sum(COALESCE(t2.Revenue_Return_30_days,0)) Revenue_Return_30_days
FROM aoi.soh_report t1 
left JOIN 
(select sku, min(order_date) as first_sale_date, max(order_date) as last_sale_date ,
COUNT(DISTINCT case when order_date between '2017-01-01' AND '2017-12-31'  AND lower(order_status) not like '%ret%' AND lower(order_status) not like '%cancel%' then sku else NULL end ) as sku_count_2017,
COUNT(DISTINCT case when order_date between '2018-01-01' AND '2018-12-31'  AND lower(order_status) not like '%ret%' AND lower(order_status) not like '%cancel%' then sku else NULL end ) as sku_count_2018,
COUNT(DISTINCT case when order_date between '2019-01-01' AND '2019-12-31'  AND lower(order_status) not like '%ret%' AND lower(order_status) not like '%cancel%' then sku else NULL end ) as sku_count_2019,
COUNT(DISTINCT case when order_date between '2020-01-01' AND '2020-12-31'  AND lower(order_status) not like '%ret%' AND lower(order_status) not like '%cancel%' then sku else NULL end ) as sku_count_2020,
sum(case when order_date between '2017-01-01' AND '2017-12-31'  AND lower(order_status) not like '%ret%' AND lower(order_status) not like '%cancel%' then COALESCE(quantity,0) else 0 end ) as Qty_sold_2017,
sum(case when order_date between '2018-01-01' AND '2018-12-31'  AND lower(order_status) not like '%ret%' AND lower(order_status) not like '%cancel%' then COALESCE(quantity,0) else 0 end ) as Qty_sold_2018,
sum(case when order_date between '2019-01-01' AND '2019-12-31' AND lower(order_status) not like '%ret%' AND lower(order_status) not like '%cancel%'  then COALESCE(quantity,0) else 0 end ) as Qty_sold_2019,
sum(case when order_date between '2020-01-01' AND '2020-12-31' AND lower(order_status) not like '%ret%' AND lower(order_status) not like '%cancel%'  then COALESCE(quantity,0) else 0 end ) as Qty_sold_2020,
sum(case when order_date between (CURRENT_DATE - INTERVAL '180 DAY') and (CURRENT_DATE - INTERVAL '1 DAY')  AND lower(order_status) not like '%ret%' AND lower(order_status) not like '%cancel%' then COALESCE(quantity,0) else 0 end ) as Qty_sold_180_days,
sum(case when order_date between (CURRENT_DATE - INTERVAL '90 DAY') and (CURRENT_DATE - INTERVAL '1 DAY') AND lower(order_status) not like '%ret%' AND lower(order_status) not like '%cancel%'  then COALESCE(quantity,0) else 0 end ) as Qty_sold_90_days,
sum(case when order_date between (CURRENT_DATE - INTERVAL '60 DAY') and (CURRENT_DATE - INTERVAL '1 DAY') AND lower(order_status) not like '%ret%' AND lower(order_status) not like '%cancel%'  then COALESCE(quantity,0) else 0 end ) as Qty_sold_60_days,
sum(case when order_date between (CURRENT_DATE - INTERVAL '30 DAY') and (CURRENT_DATE - INTERVAL '1 DAY')  AND lower(order_status) not like '%ret%' AND lower(order_status) not like '%cancel%' then COALESCE(quantity,0) else 0 end ) as Qty_sold_30_days,
sum(case when order_date between '2017-01-01' AND '2017-12-31' AND lower(order_status) not like '%ret%' AND lower(order_status) not like '%cancel%' then COALESCE(net_sale_price_kwd,0) else 0 end ) as Revenue_2017,
sum(case when order_date between '2018-01-01' AND '2018-12-31' AND lower(order_status) not like '%ret%' AND lower(order_status) not like '%cancel%' then COALESCE(net_sale_price_kwd,0) else 0 end ) as Revenue_2018,
sum(case when order_date between '2019-01-01' AND '2019-12-31'  AND lower(order_status) not like '%ret%' AND lower(order_status) not like '%cancel%' then COALESCE(net_sale_price_kwd,0) else 0 end ) as Revenue_2019,
sum(case when order_date between '2020-01-01' AND '2020-12-31'  AND lower(order_status) not like '%ret%' AND lower(order_status) not like '%cancel%' then COALESCE(net_sale_price_kwd,0) else 0 end ) as Revenue_2020,
sum(case when order_date between (CURRENT_DATE - INTERVAL '180 DAY') and (CURRENT_DATE - INTERVAL '1 DAY')  AND lower(order_status) not like '%ret%' AND lower(order_status) not like '%cancel%' then COALESCE(net_sale_price_kwd,0) else 0 end ) as Revenue_180_days,
sum(case when order_date between (CURRENT_DATE - INTERVAL '90 DAY') and (CURRENT_DATE - INTERVAL '1 DAY') AND lower(order_status) not like '%ret%' AND lower(order_status) not like '%cancel%'  then COALESCE(net_sale_price_kwd,0) else 0 end ) as Revenue_90_days,
sum(case when order_date between (CURRENT_DATE - INTERVAL '60 DAY') and (CURRENT_DATE - INTERVAL '1 DAY') AND lower(order_status) not like '%ret%' AND lower(order_status) not like '%cancel%'  then COALESCE(net_sale_price_kwd,0) else 0 end ) as Revenue_60_days,
sum(case when order_date between (CURRENT_DATE - INTERVAL '30 DAY') and (CURRENT_DATE - INTERVAL '1 DAY') AND lower(order_status) not like '%ret%' AND lower(order_status) not like '%cancel%' then COALESCE(net_sale_price_kwd,0) else 0 end ) as Revenue_30_days,
sum(case when order_date between '2017-01-01' AND '2017-12-31' AND lower(order_status) like '%cancel%'
then COALESCE(quantity,0) else 0 end ) as Qty_Cancel_2017,
sum(case when order_date between '2018-01-01' AND '2018-12-31' AND lower(order_status) like '%cancel%'
then COALESCE(quantity,0) else 0 end ) as Qty_Cancel_2018,
sum(case when order_date between '2019-01-01' AND '2019-12-31' AND lower(order_status) like '%cancel%'
then COALESCE(quantity,0) else 0 end ) as Qty_Cancel_2019,
sum(case when order_date between '2020-01-01' AND '2020-12-31' AND lower(order_status) like '%cancel%'
then COALESCE(quantity,0) else 0 end ) as Qty_Cancel_2020,
sum(case when order_date between (CURRENT_DATE - INTERVAL '180 DAY') and (CURRENT_DATE - INTERVAL '1 DAY') AND lower(order_status) like '%cancel%' then COALESCE(quantity,0) else 0 end ) as Qty_Cancel_180_days,
sum(case when order_date between (CURRENT_DATE - INTERVAL '90 DAY') and (CURRENT_DATE - INTERVAL '1 DAY') AND lower(order_status) like '%cancel%'then COALESCE(quantity,0) else 0 end ) as Qty_Cancel_90_days,
sum(case when order_date between (CURRENT_DATE - INTERVAL '60 DAY') and (CURRENT_DATE - INTERVAL '1 DAY') AND lower(order_status) like '%cancel%'then COALESCE(quantity,0) else 0 end ) as Qty_Cancel_60_days,
sum(case when order_date between (CURRENT_DATE - INTERVAL '30 DAY') and (CURRENT_DATE - INTERVAL '1 DAY')
AND lower(order_status) like '%cancel%' then COALESCE(quantity,0) else 0 end ) as Qty_Cancel_30_days,
sum(case when order_date between '2017-01-01' AND '2017-12-31' AND lower(order_status) like '%cancel%' then COALESCE(net_sale_price_kwd,0) else 0 end ) as Revenue_Cancel_2017,
sum(case when order_date between '2018-01-01' AND '2018-12-31' AND lower(order_status) like '%cancel%' then COALESCE(net_sale_price_kwd,0)else 0 end ) as Revenue_Cancel_2018,
sum(case when order_date between '2019-01-01' AND '2019-12-31' AND lower(order_status) like '%cancel%' then COALESCE(net_sale_price_kwd,0) else 0 end ) as Revenue_Cancel_2019,
sum(case when order_date between '2020-01-01' AND '2020-12-31' AND lower(order_status) like '%cancel%' then COALESCE(net_sale_price_kwd,0) else 0 end ) as Revenue_Cancel_2020,
sum(case when order_date between (CURRENT_DATE - INTERVAL '180 DAY') and (CURRENT_DATE - INTERVAL '1 DAY') AND lower(order_status) like '%cancel%' then COALESCE(net_sale_price_kwd,0) else 0 end ) as Revenue_Cancel_180_days,
sum(case when order_date between (CURRENT_DATE - INTERVAL '90 DAY') and (CURRENT_DATE - INTERVAL '1 DAY')AND lower(order_status) like '%cancel%' then COALESCE(net_sale_price_kwd,0) else 0 end ) as Revenue_Cancel_90_days,
sum(case when order_date between (CURRENT_DATE - INTERVAL '60 DAY') and (CURRENT_DATE - INTERVAL '1 DAY') AND lower(order_status) like '%cancel%'then COALESCE(net_sale_price_kwd,0) else 0 end ) as Revenue_Cancel_60_days,
sum(case when order_date between (CURRENT_DATE - INTERVAL '30 DAY') and (CURRENT_DATE - INTERVAL '1 DAY') AND lower(order_status) like '%cancel%'then COALESCE(net_sale_price_kwd,0) else 0 end ) as Revenue_Cancel_30_days,
sum(case when order_date between '2017-01-01' AND '2017-12-31' AND lower(order_status) like '%ret%'
then COALESCE(quantity,0) else 0 end ) as Qty_Return_2017,
sum(case when order_date between '2018-01-01' AND '2018-12-31' AND lower(order_status) like '%ret%'
then COALESCE(quantity,0) else 0 end ) as Qty_Return_2018,
sum(case when order_date between '2019-01-01' AND '2019-12-31' AND lower(order_status) like '%ret%'
then COALESCE(quantity,0) else 0 end ) as Qty_Return_2019,
sum(case when order_date between '2020-01-01' AND '2020-12-31' AND lower(order_status) like '%ret%'
then COALESCE(quantity,0) else 0 end ) as Qty_Return_2020,
sum(case when order_date between (CURRENT_DATE - INTERVAL '180 DAY') and (CURRENT_DATE - INTERVAL '1 DAY') AND lower(order_status) like '%ret%' then COALESCE(quantity,0) else 0 end ) as Qty_Return_180_days,
sum(case when order_date between (CURRENT_DATE - INTERVAL '90 DAY') and (CURRENT_DATE - INTERVAL '1 DAY') AND lower(order_status) like '%ret%'then COALESCE(quantity,0) else 0 end ) as Qty_Return_90_days,
sum(case when order_date between (CURRENT_DATE - INTERVAL '60 DAY') and (CURRENT_DATE - INTERVAL '1 DAY') AND lower(order_status) like '%ret%'then COALESCE(quantity,0) else 0 end ) as Qty_Return_60_days,
sum(case when order_date between (CURRENT_DATE - INTERVAL '30 DAY') and (CURRENT_DATE - INTERVAL '1 DAY')
AND lower(order_status) like '%ret%' then COALESCE(quantity,0)else 0 end ) as Qty_Return_30_days,
sum(case when order_date between '2017-01-01' AND '2017-12-31' AND lower(order_status) like '%ret%' then COALESCE(net_sale_price_kwd,0) else 0 end ) as Revenue_Return_2017,
sum(case when order_date between '2018-01-01' AND '2018-12-31' AND lower(order_status) like '%ret%' then COALESCE(net_sale_price_kwd,0) else 0 end ) as Revenue_Return_2018,
sum(case when order_date between '2019-01-01' AND '2019-12-31' AND lower(order_status) like '%ret%' then COALESCE(net_sale_price_kwd,0) else 0 end ) as Revenue_Return_2019,
sum(case when order_date between '2020-01-01' AND '2020-12-31' AND lower(order_status) like '%ret%' then COALESCE(net_sale_price_kwd,0) else 0 end ) as Revenue_Return_2020,
sum(case when order_date between (CURRENT_DATE - INTERVAL '180 DAY') and (CURRENT_DATE - INTERVAL '1 DAY') AND lower(order_status) like '%ret%' then COALESCE(net_sale_price_kwd,0) else 0 end ) as Revenue_Return_180_days,
sum(case when order_date between (CURRENT_DATE - INTERVAL '90 DAY') and (CURRENT_DATE - INTERVAL '1 DAY')AND lower(order_status) like '%ret%' then COALESCE(net_sale_price_kwd,0) else 0 end ) as Revenue_Return_90_days,
sum(case when order_date between (CURRENT_DATE - INTERVAL '60 DAY') and (CURRENT_DATE - INTERVAL '1 DAY') AND lower(order_status) like '%ret%' then COALESCE(net_sale_price_kwd,0) else 0 end ) as Revenue_Return_60_days,
sum(case when order_date between (CURRENT_DATE - INTERVAL '30 DAY') and (CURRENT_DATE - INTERVAL '1 DAY') AND lower(order_status) like '%ret%' then COALESCE(net_sale_price_kwd,0) else 0 end ) as Revenue_Return_30_days
from aoi.order_items
where date(order_date) >= '2017-01-01'
and lower(order_category) ='normal'
group by 1 ) t2
ON t1.sku = t2.sku
group by 1,2,3,4,5,6
);

COMMIT;
