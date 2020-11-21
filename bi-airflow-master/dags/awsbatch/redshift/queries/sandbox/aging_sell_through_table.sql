BEGIN;
DROP TABLE IF EXISTS sandbox.aging_sell_through_table;
SELECT * INTO sandbox.aging_sell_through_table
FROM
(
SELECT t.*,
(CASE 
     WHEN (t.age) <=30 THEN 'A. 0-30'
     WHEN (t.age)<=60 THEN 'B. 30-60'
     WHEN (t.age)<=90 THEN 'C. 60-90'
     WHEN (t.age)<=120 THEN 'D. 90-120'
     WHEN (t.age)<=150 THEN 'E. 120-150'
     WHEN (t.age)<=180 THEN 'F. 150-180'
     WHEN (t.age)>180 THEN 'G. 180+'
     ELSE  'H. Age Missing'
END) age_bucket,
(CASE 
      WHEN (t.sale_from_lastgrn)=0 THEN 'A. No Sale'
      WHEN (t.sell_through_percent) <=0.15 THEN 'B. 0-15%'
      WHEN (t.sell_through_percent)<=0.30 THEN 'C. 15-30%'
      WHEN (t.sell_through_percent)<=0.45 THEN 'D. 30-45%'
      WHEN (t.sell_through_percent)<=0.60 THEN 'E. 45-60%'
      WHEN (t.sell_through_percent)<=0.75 THEN 'F. 60-75%'
      WHEN (t.sell_through_percent)<=0.90 THEN 'G. 75-90%'
      ELSE 'H. +90%'
END) sell_through_bucket,
(CASE WHEN t.soh<=0 or t.soh IS NULL THEN '0'
      ELSE '1'
END) inventory_flag,
GETDATE() as synched_at_utc
FROM
(
SELECT
      sale_and_soh.*,
      COALESCE(sale_and_soh.soh,0)*COALESCE(sale_and_soh.cost_price,0)stock_value,
      datediff(day, sale_and_soh.last_grn_date, CURRENT_DATE) as age ,
     ROUND(cast(sale_and_soh.sale_from_lastgrn as float)/cast(NULLIF((COALESCE(sale_and_soh.sale_from_lastgrn,0)+COALESCE(sale_and_soh.soh,0)),0)as float),3) sell_through_percent
FROM
     (SELECT
             soh.sku as soh_sku,
             min(soh.brand) as brand,
             min(soh.category1) as category1,
             min(soh.category2) as category2,
             min(soh.category3) as category3,
             min(soh.category4) as category4,
             min(soh.department) as department,
             min(date(soh.first_grn_date)) first_grn_date,
             min(date(soh.last_grn_date)) last_grn_date,
             min(COALESCE(soh.soh,0)) soh,
             min(COALESCE(soh.cost_price,0)) cost_price,
             min(COALESCE(soh.open_po_total_qty,0)) open_po_total_qty,
             sum(case when sale.order_date between date(soh.last_grn_date) and (CURRENT_DATE - INTERVAL '1 DAY')then COALESCE(sale.quantity,0) else 0 end ) as sale_from_lastgrn,
             sum(case when sale.order_date between (CURRENT_DATE - INTERVAL '7 DAYS') and(CURRENT_DATE - INTERVAL '1 DAY')then COALESCE(sale.quantity,0) else 0 end ) as qtysold7day,
             sum(case when sale.order_date between (CURRENT_DATE - INTERVAL '14 DAYS') and(CURRENT_DATE - INTERVAL '1 DAY')then COALESCE(sale.quantity,0) else 0 end ) as qtysold14day,
             sum(case when sale.order_date between (CURRENT_DATE - INTERVAL '30 DAYS') and(CURRENT_DATE - INTERVAL '1 DAY')then COALESCE(sale.quantity,0) else 0 end ) as qtysold30day,
             sum(case when sale.order_date between (CURRENT_DATE - INTERVAL '60 DAYS') and(CURRENT_DATE - INTERVAL '1 DAY')then COALESCE(sale.quantity,0) else 0 end ) as qtysold60day,
             sum(case when sale.order_date between (CURRENT_DATE - INTERVAL '90 DAYS') and(CURRENT_DATE - INTERVAL '1 DAY')then COALESCE(sale.quantity,0) else 0 end ) as qtysold90day,
             sum(case when sale.order_date between (CURRENT_DATE - INTERVAL '120 DAYS') and(CURRENT_DATE - INTERVAL '1 DAY')then COALESCE(sale.quantity,0) else 0 end ) as qtysold120day,
             sum(COALESCE(sale.quantity,0)) as qtysince_launch
       from aoi.soh_report soh
left join
(select * from aoi.order_items where sku is null OR
(order_category<>'CELEBRITY' and order_status not like '%Cancel%' and order_status not like '%Ret%'))sale
on soh.sku=sale.sku
GROUP BY soh_sku) sale_and_soh) t
);
COMMIT;

