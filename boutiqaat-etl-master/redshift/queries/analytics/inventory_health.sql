BEGIN;
DELETE analytics.inventory_health where 1=1;

SELECT * INTO analytics.inventory_health
FROM
(
select 
ot.sku, ot.parent_sku, 
ot.bar_code, ot.nav_sku_name, ot.brand, ot.department, ot.category1, ot.category2, 
ot.vendor_no, ot.vendor_name, ot.first_grn_date, ot.first_Order_Date, 
ot.Last_Order_Date, ot.soh_wh, ot.Open_PO_Qty,
ot.crs_available, ot.First_Sale_in_60Days, ot.days_sold_60days ,ot.sold_qty_07days,
ot.sold_qty_14days, 
ot.Sold_QTY_30Days, ot.Sold_QTY_60Days, ot.Sold_QTY_90Days, ot.Sold_QTY_180Days, 
ot.lifetime_sold_qty, ot.Gift_QTY, 
ot.StockCover_60days_sale_Basis, ot.StockCover_60days_Including_OpenPO,
(Case when (ot.First_Sale_in_60Days='Yes' or ot.First_GRN_in_60Days='Yes') then 
'New_SKU' else 'Old_SKU' end) Old_New_SKU, 
(case when (ot.First_Sale_in_60Days='Yes' or ot.First_GRN_in_60Days='Yes') then 
'New_SKU'
when ot.First_Sale_in_60Days='No' and ot.Sold_QTY_60Days='0' then 'Non_Moving'
when ot.First_Sale_in_60Days='No' and ot.Sold_QTY_60Days >'0' and 
ot.StockCover_60days_sale_Basis <2 then 'Low_Cover'
when ot.First_Sale_in_60Days='No' and ot.Sold_QTY_60Days >'0' and 
ot.StockCover_60days_sale_Basis <3 then 'Normal_Cover'
when ot.First_Sale_in_60Days='No' and ot.Sold_QTY_60Days >'0' and 
ot.StockCover_60days_sale_Basis >=3 then 'High_Cover'
else 'Exceptions' end ) Stock_Cover_Flag,
(case when (ot.First_Sale_in_60Days='Yes' or ot.First_GRN_in_60Days='Yes') then 
'New_SKU'
when ot.First_Sale_in_60Days='No' and ot.Sold_QTY_60Days='0' then 'Non_Moving'
when ot.First_Sale_in_60Days='No' and ot.Sold_QTY_60Days >'0' and 
ot.StockCover_60days_Including_OpenPO <2 then 'Low_Cover'
when ot.First_Sale_in_60Days='No' and ot.Sold_QTY_60Days >'0' and 
ot.StockCover_60days_Including_OpenPO <3 then 'Normal_Cover'
when ot.First_Sale_in_60Days='No' and ot.Sold_QTY_60Days >'0' and 
ot.StockCover_60days_Including_OpenPO >=3 then 'High_Cover'
else 'Exceptions' end ) Stock_Cover_Flag_Including_OpenPO,
(case when (ot.First_Sale_in_60Days='Yes' or ot.First_GRN_in_60Days='Yes') then 
'New_SKU'
when ot.First_Sale_in_60Days='No' and ot.Sold_QTY_60Days='0' then 'Non_Moving'
when ot.First_Sale_in_60Days='No' and ot.Sold_QTY_60Days <='30' then 'Slow_Moving'
when ot.First_Sale_in_60Days='No' and ot.Sold_QTY_60Days <'200' then 
'Normal_Moving'
when ot.First_Sale_in_60Days='No' and ot.Sold_QTY_60Days >='200' then 'Fast_Moving'
else 'Not_Defined' end ) Inventory_Category,
datediff(day,ot.first_Order_Date, ot.first_grn_date) Firt_GRN_to_First_Sale_Days,
ot.SKU_Live_Status, ot.Events_Count_30days, ot.Events_Count_60days, 
ot.Events_Count_90days
from (select soh.sku, soh.parent_sku, soh.bar_code, soh.nav_sku_name, soh.brand, 
soh.department, soh.category1, soh.category2, 
soh.vendor_no, soh.vendor_name, soh.first_grn_date, od.first_Order_Date 
first_Order_Date, od.Last_Order_Date Last_Order_Date, 
 soh.First_GRN_in_60Days,
 cast((case when mg.value='1' THEN 'Live' else 'Not_Live' end) as char(20)) 
SKU_Live_Status,
(case when od.first_Order_Date >= (CURRENT_DATE - INTERVAL '60 DAY') then 'Yes' 
else 'No' end) First_Sale_in_60Days,
 sum(COALESCE(soh.crs_available,0)) crs_available,
sum(COALESCE(er.Events_Count_30days,0)) Events_Count_30days , 
sum(COALESCE(er.Events_Count_60days,0)) Events_Count_60days , 
 sum(COALESCE(er.Events_Count_90days,0)) Events_Count_90days,
sum(COALESCE(od.days_sold_60days,0)) days_sold_60days, 
sum(COALESCE((od.sold_qty_07days),0)) sold_qty_07days, 
sum(COALESCE((od.sold_qty_14days),0)) sold_qty_14days, 
sum(COALESCE((od.sold_qty_30days),0)) Sold_QTY_30Days, 
sum(COALESCE((od.sold_qty_60days),0)) Sold_QTY_60Days, 
sum(COALESCE((od.sold_qty_90days),0)) Sold_QTY_90Days,
 sum(COALESCE((od.sold_qty_180days),0)) Sold_QTY_180Days, 
 sum(COALESCE(od.lifetime_sold_qty,0)) lifetime_sold_qty, 
sum(COALESCE((Total_Gift_QTY),0)) Gift_QTY,
case when sum(COALESCE((od.sold_qty_60days),0))=0 and 
sum(COALESCE(od.lifetime_sold_qty,0)) >0 then 'nil_sale_60_days'
       when sum(COALESCE(od.lifetime_sold_qty,0)) =0 then 'lifetime_no_sale'
       when sum(COALESCE(soh.soh_wh))=0 then 'No_stock'
      else cast((sum(COALESCE(soh.soh_wh))/sum(COALESCE(od.sold_qty_60days,0)))/
30.42*datediff(day,
               greatest(od.first_Order_Date,(CURRENT_DATE - INTERVAL '60 
DAY')),CURRENT_DATE)as char(10)) end StockCover_60days_sale_Basis,
case when sum(COALESCE(od.sold_qty_60days,0))=0 then 'nil_sale_60_days'
     when sum(COALESCE(od.lifetime_sold_qty,0)) =0 then 'lifetime_no_sale'
    when sum(COALESCE(soh.soh_wh,0) +COALESCE(Open_PO_Qty,0))=0 then 'No_stock'
    else cast(((sum(COALESCE(soh.soh_wh,0)) + sum(COALESCE(Open_PO_Qty,0))))/ 
sum(COALESCE((od.sold_qty_60days),0))/30.42*datediff(day,greatest(od.first_Order_Da
te,(CURRENT_DATE - INTERVAL '60 DAY')),CURRENT_DATE)
       as char(10)) end StockCover_60days_Including_OpenPO,
 sum(COALESCE(soh.soh_wh,0)) soh_wh, sum(COALESCE(Open_PO_Qty,0)) Open_PO_Qty
from (select t1.sku, COALESCE(so.config_sku, t1.sku) parent_sku, bar_code, 
nav_sku_name, brand, department, category1, 
   category2, vendor_no, vendor_name, first_grn_date, sum(crs_available) 
crs_available, 
 (case when first_grn_date >= (CURRENT_DATE - INTERVAL '60 DAY') then 'Yes' else 
'No' end) First_GRN_in_60Days,
  sum(COALESCE(total_sellable_qty,0)-COALESCE(qty_not_picked_not_cancelled,0)) 
soh_wh,
   sum((COALESCE(full_pending_open_po_qty, 0)+ 
COALESCE(partially_pending_open_po_qty,0))) Open_PO_Qty
  FROM (select sku from analytics.soh_report  so group by sku
        UNION
        select sku from aoi.order_items oi group by sku) t1 
   left join analytics.soh_report  so on t1.sku=so.sku
   group by 1,2,3,4,5,6,7,8,9,10,11) soh
left join 
( select so.sku, so.parent_sku, min(first_Order_Date) first_Order_Date, 
max(Last_Order_Date) Last_Order_Date , sum(COALESCE(days_sold_60days,0)) 
days_sold_60days, sum(COALESCE(sold_qty_07days,0)) sold_qty_07days,
sum(COALESCE(sold_qty_14days,0)) sold_qty_14days, sum(COALESCE(sold_qty_30days,0)) 
sold_qty_30days, sum(COALESCE(sold_qty_60days,0)) sold_qty_60days, 
sum(COALESCE(sold_qty_90days,0)) sold_qty_90days, 
 sum(COALESCE(sold_qty_180days,0)) sold_qty_180days, 
sum(COALESCE(lifetime_sold_qty,0)) lifetime_sold_qty, 
sum(COALESCE(Total_Gift_QTY,0)) Total_Gift_QTY
from (select ab.sku, COALESCE(so.config_sku, ab.sku) parent_sku
  from (select sku from analytics.soh_report  so group by sku
        UNION
        select sku from aoi.order_items oi group by sku) ab 
   left join analytics.soh_report  so on ab.sku=so.sku
group by 1,2) so
left join (select sku, min(case when lower(order_category) ='normal'
and lower(order_status) not like '%cancel%' and lower(order_status) not like '%ret
%' then order_date else NULL end) first_Order_Date ,
max(case when lower(order_category) ='normal'
and lower(order_status) not like '%cancel%' and lower(order_status) not like '%ret
%' then order_date else NULL end) Last_Order_Date ,
count(distinct case when lower(order_category) ='normal'
and lower(order_status) not like '%cancel%' and lower(order_status) not like '%ret
%' and order_date >= (CURRENT_DATE - INTERVAL '60 DAY') then order_date else NULL 
end) days_sold_60days,
sum(case when lower(order_category) ='normal'
and lower(order_status) not like '%cancel%' and lower(order_status) not like '%ret
%' and order_date >= (CURRENT_DATE - INTERVAL '07 DAY') then quantity else 0 end) 
sold_qty_07days, 
sum(case when lower(order_category) ='normal'
and lower(order_status) not like '%cancel%' and lower(order_status) not like '%ret
%' and order_date >= (CURRENT_DATE - INTERVAL '14 DAY') then quantity else 0 end) 
sold_qty_14days,
sum(case when lower(order_category) ='normal'
and lower(order_status) not like '%cancel%' and lower(order_status) not like '%ret
%' and order_date >= (CURRENT_DATE - INTERVAL '30 DAY') then quantity else 0 end) 
sold_qty_30days, sum(case when lower(order_category) ='normal'
and lower(order_status) not like '%cancel%' and lower(order_status) not like '%ret
%' and order_date >= (CURRENT_DATE - INTERVAL '60 DAY') then quantity else 0 end) 
sold_qty_60days, sum(case when lower(order_category) ='normal'
and lower(order_status) not like '%cancel%' and lower(order_status) not like '%ret
%' and order_date >= (CURRENT_DATE - INTERVAL '90 DAY') then quantity else 0 end) 
sold_qty_90days, sum(case when lower(order_category) ='normal'
and lower(order_status) not like '%cancel%' and lower(order_status) not like '%ret
%' and order_date >= (CURRENT_DATE - INTERVAL '180 DAY') then quantity else 0 end) 
sold_qty_180days, sum(case when lower(order_category) ='normal'
and lower(order_status) not like '%cancel%' and lower(order_status) not like '%ret
%' then quantity else 0 end) lifetime_sold_qty, 
sum(case when lower(order_category) !='normal'
and lower(order_status) not like '%cancel%' and lower(order_status) not like '%ret
%' then quantity else 0 end) Total_Gift_QTY 
from aoi.order_items 
 where date(order_date) <= CURRENT_DATE
group by 1 ) oi on so.sku=oi.sku
group by 1,2) od on soh.sku=od.sku
left join ( SELECT cpf.sku sku, cpe.attribute_id,cpe.value value
from magento.catalog_product_flat_1 cpf
left JOIN magento.catalog_product_entity_int cpe on cpf.row_id=cpe.row_id
where cpe.attribute_id=96 and cpe.value=1
group by 1,2,3 ) mg on soh.sku=mg.sku 
left join (select sku_code, COUNT(DISTINCT case when er.event_date >=(CURRENT_DATE 
- INTERVAL '30 DAY') then er.event_id else NULL end) Events_Count_30days,
COUNT(DISTINCT case when er.event_date >= (CURRENT_DATE - INTERVAL '60 DAY') then 
er.event_id else NULL end) Events_Count_60days,
COUNT(DISTINCT case when er.event_date >= (CURRENT_DATE - INTERVAL '90 DAY') then 
er.event_id else NULL end) Events_Count_90days
FROM aoi.events_report er
where lower(er.generic) like '%sku%'
and er.event_date <= CURRENT_DATE GROUP by 1)  er 
on soh.sku=er.sku_code
group by 1 ,2,3,4,5,6,7,8,9,10 ,11,12 ,13 ,14 ,15) ot
group by 
1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31
,32,33,34,35,36,37);

COMMIT;