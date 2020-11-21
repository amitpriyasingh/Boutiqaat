set @yesterday = subdate(current_date, interval 1 day) ; 

create temporary table complete_sku as select distinct i.PRODN sku, i.PAR_CODE barcode, i.PRODNAME_E sku_name, i.last_grn_date last_grn_date, cat1.NAME_E cat1, cat2.NAME_E cat2, cat3.NAME_E cat3, cat4.NAME_E cat4, br.NAME_E brand, i.VENDOR supplier_id, 1 store_id, ip.qty quantity, ip.location location,
    r2.grn_no grn_no,
    r1.payment payment_term,
    ip.cost total_cost
    FROM items i 
    LEFT JOIN (select max(BILNO) grn_no, PRODN from reciept_2 r2 where CODE=3 group by PRODN) r2 on r2.PRODN = i.PRODN
    LEFT JOIN (select PAYMENT payment, BILNO bilno from reciept_1 where CODE=3 group by bilno) r1 on r1.bilno=r2.grn_no
    LEFT JOIN PRODUCT_CAT1 cat1 ON i.CAT1 = cat1.ID
    LEFT JOIN PRODUCT_CAT2 cat2 ON i.CAT2 = cat2.ID
    LEFT JOIN PRODUCT_CAT3 cat3 ON i.CAT3 = cat3.ID
    LEFT JOIN PRODUCT_CAT4 cat4 ON i.CAT4 = cat4.ID
    LEFT JOIN MK_BRANDS br ON i.BRAND_ID = br.ID
    LEFT JOIN item_prods ip ON ip.PRODN = i.PRODN AND ip.PRODS = 1 group by sku;

create temporary table sku_gross as select distinct sku, 
    sum(if(order_date = subdate(@yesterday, interval 2 day), quantity, 0)) gq_1, 
    sum(if(order_date > subdate(@yesterday, interval 8 day), quantity, 0)) gq_7, 
    sum(if(order_date > subdate(@yesterday, interval 15 day), quantity, 0)) gq_14, 
    sum(if(order_date > subdate(@yesterday, interval 31 day), quantity, 0)) gq_31, 
    sum(if(year(order_date) = year(@yesterday), quantity, 0)) gq_year, 
    sum(quantity) gq,
    sum(if(order_date = subdate(@yesterday, interval 2 day), item_total, 0)) gr_1, 
    sum(if(order_date > subdate(@yesterday, interval 8 day), item_total, 0)) gr_7, 
    sum(if(order_date > subdate(@yesterday, interval 15 day), item_total, 0)) gr_14, 
    sum(if(order_date > subdate(@yesterday, interval 31 day), item_total, 0)) gr_31, 
    sum(if(year(order_date) = year(@yesterday), item_total, 0)) gr_year, 
    sum(item_total) gr,
    sum(if(order_date = subdate(@yesterday, interval 2 day), (item_total- quantity*item_cost), 0)) gcm_1, 
    sum(if(order_date > subdate(@yesterday, interval 8 day), (item_total- quantity*item_cost), 0)) gcm_7, 
    sum(if(order_date > subdate(@yesterday, interval 15 day), (item_total- quantity*item_cost), 0)) gcm_14, 
    sum(if(order_date > subdate(@yesterday, interval 31 day), (item_total- quantity*item_cost), 0)) gcm_31, 
    sum(if(year(order_date) = year(@yesterday), (item_total- quantity*item_cost), 0)) gcm_year, 
    sum(item_total- quantity*item_cost) gcm
    FROM aoi where sku group by sku ;

INSERT INTO stock_details
(
report_date,
sku,
supplier_barcode,
product_name,
category1,            
category2,            
category3,            
category4,
brand,
location,
grn_no,
vendor_code,
grn_date,
payment_term,
unit_price,
list_price,
quantity,
total_cost,
is_visible, 
is_exclusive,
is_celebexclusive, 
celebrity_name, 
age, 
grossqtysold_lastday, 
grossqtysold_7days, 
grossqtysold_14days, 
grossqtysold_30days, 
grossqtysold_ytd, 
grossqtysold_total, 
grossrevenue_lastday, 
grossrevenue_7days, 
grossrevenue_14days, 
grossrevenue_30days, 
grossrevenue_ytd, 
grossrevenue_total, 
grosscm1_lastday, 
grosscm1_7days, 
grosscm1_14days, 
grosscm1_30days, 
grosscm1_ytd, 
grosscm1_total 
) 
SELECT 
subdate(current_date, interval 1 day),
cs.sku,
cs.barcode,
cs.sku_name,
cs.cat1,
cs.cat2,
cs.cat3,
cs.cat4,
cs.brand,
cs.store_id,
cs.grn_no,
cs.supplier_id,
cs.last_grn_date,
cs.payment_term,
c1.price,
c1.special_price,
cs.quantity,
cs.total_cost,
if(c1.visibility in (4,3),1,0),
c1.exclusive,
if(cp.is_exclusive='Yes',1,0),
if(cp.is_exclusive='Yes', celeb.celebrity_name, ''),
datediff(current_date, cs.last_grn_date), 
coalesce(sg.gq_1, 0),
coalesce(sg.gq_7, 0),
coalesce(sg.gq_14, 0),
coalesce(sg.gq_31, 0),
coalesce(sg.gq_year,0),
coalesce(sg.gq, 0),
coalesce(sg.gr_1, 0),
coalesce(sg.gr_7, 0),
coalesce(sg.gr_14, 0),
coalesce(sg.gr_31, 0),
coalesce(sg.gr_year, 0),
coalesce(sg.gr, 0),
coalesce(sg.gcm_1, 0),
coalesce(sg.gcm_7, 0),
coalesce(sg.gcm_14, 0),
coalesce(sg.gcm_31, 0),
coalesce(sg.gcm_year, 0),
coalesce(sg.gcm, 0)
FROM complete_sku cs 
LEFT JOIN sku_gross sg ON sg.sku = cs.sku 
LEFT JOIN catalog_product_flat_1 c1 ON c1.sku = cs.sku 
LEFT JOIN celebrity_product cp ON cp.product_entity_id = c1.entity_id 
LEFT JOIN celebrity_master celeb ON celeb.celebrity_id = cp.celebrity_id 
GROUP BY cs.sku;