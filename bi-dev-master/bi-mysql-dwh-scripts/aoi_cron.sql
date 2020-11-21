insert into
aoi ( order_date, yearmonth , order_year , order_week , order_month , branch_id , store_id , order_number , device_type , payment_method_id , payment_method , line_id , sku , sku_name , sku_barcode , item_cost , new_price , sales_price , unit_price , last_order_price , category1 , category2, category3 , category4 , brand_id , brand_name , purchase_type, box_id , box_price , box_discount , box_name , discount , quantity , box_quantity , item_total , shipping_charge , order_discount , order_total , order_gmv , customer_name , customer_telephone , order_mobile , is_cancelled , is_returned , is_b_fulfilled , order_ref_no , celebrity_name , celebrity_id , celebrity_price , celebrity_commission , order_region , city , country , is_celebrity , currency_id , currency , currency_rate , ex_rate , awbstatus , awbnumber , return_awbnumber , driver_id , runsheet_no , non_inventory , status , status_group,supplier_id , supplier_name , supplier_acc_no , soh , sales_period , last_grn_date, ageing, ageing_month, customer_id
)
(SELECT 
ISSUE_1.VDATE,
extract(year_month from ISSUE_1.VDATE),
year(ISSUE_1.VDATE),
week(ISSUE_1.VDATE),
month(ISSUE_1.VDATE),
ISSUE_1.PCENTER BRANCHID,
ISSUE_2.PRODS,
ISSUE_1.BILNO cust,
ISSUE_1.DV_OS_TYPE,
(CASE ISSUE_1.PAYMENT WHEN 0 THEN 2 ELSE ISSUE_1.PAYMENT END ) ,
(CASE ISSUE_1.PAYMENT WHEN 0 THEN 'CASH on Delivery' ELSE CASH_PAYMENTS.NAME_E END),
ISSUE_2.ID LINEID,
ITEMS.PRODN SKU,
ITEMS.PRODNAME_E,
ITEMS.PAR_CODE,
(CASE ITEM_PRODS.COST WHEN 0 THEN ISSUE_2.ITEM_COST ELSE ITEM_PRODS.COST END ) ,
ITEMS.SAL_PRICE,
(ITEMS.SAL_PRICE*ISSUE_2.PRICE/(bp.cal_price))*(CASE ITEMS_ASSEM.FREE_PRICE_SW WHEN 1 THEN 0 ELSE 1 END ),
ITEMS.SAL_PRICE,
ITEMS.LAST_ORDER_PRICE,
PRODUCT_CAT1.NAME_E,
PRODUCT_CAT2.NAME_E,
PRODUCT_CAT3.NAME_E,
PRODUCT_CAT4.NAME_E,
ITEMS.BRAND_ID,
IFNULL(MK_BRANDS.NAME_E,ITEMS.BRAND_NAME),
brand_purchase_type_master.purchase_type,
ISSUE_2.PRODN,
box_ITEMS.SAL_PRICE,
ISSUE_2.DISC2,
box_ITEMS.PRODNAME_E,
(ITEMS.SAL_PRICE - (ITEMS.SAL_PRICE*ISSUE_2.PRICE/(bp.cal_price))*(CASE ITEMS_ASSEM.FREE_PRICE_SW WHEN 1 THEN 0 ELSE 1 END ) )*100/ITEMS.SAL_PRICE,
(ISSUE_2.QTY * ITEMS_ASSEM.QTY),
ISSUE_2.QTY,
(ITEMS.SAL_PRICE*ISSUE_2.PRICE/(bp.cal_price))*(ITEMS_ASSEM.QTY*ISSUE_2.QTY)*(CASE ITEMS_ASSEM.FREE_PRICE_SW WHEN 1 THEN 0 ELSE 1 END ),
shipping.charge,
ISSUE_1.DISCOUNT,
ISSUE_1.TOTAL,
(ISSUE_1.TOTAL - ISSUE_1.DISCOUNT),
ISSUE_1.CUSTOMER_NAME,
ISSUE_1.CUST_TEL,
ISSUE_1.ORDER_MOBILE,
(CASE ISSUE_1.ORDER_STAT WHEN 'Canceled' THEN 1 WHEN 'CancelRq' THEN 1 ELSE 0 END),
(CASE ISSUE_1.ORDER_STAT WHEN 'Returned' THEN 1 ELSE 0 END ),
(CASE ISSUE_1.ORDER_STAT WHEN 'Deliverd' THEN 1 ELSE 0 END ),
ISSUE_1.ORDER_REF_NO,
MK_CELEB.NAME_E,
ISSUE_2.CATEGORY_ID,
ISSUE_2.CELEBRITY_PRICE,
0.1*(ITEMS.SAL_PRICE*ISSUE_2.TOTAL/(ISSUE_2.QTY*bp.cal_price))*(ITEMS_ASSEM.QTY*ISSUE_2.QTY)*(CASE ITEMS_ASSEM.FREE_PRICE_SW WHEN 1 THEN 0 ELSE 1 END ),
ISSUE_1.ORDER_REGION,
ISSUE_1.ORDER_AREA,
centers.NAME_E,
(CASE ISSUE_2.CATEGORY_ID WHEN NULL THEN 0 ELSE 1 END ),
ISSUE_1.CURR_ID,
CURRANCY.NAME_E,
ISSUE_1.CURR_RATE,
CURRANCY.EX_RATE,
ISSUE_1.AWBSTATUS,
ISSUE_1.AWBNUMBER,
ISSUE_1.RETURN_AWBNUMBER,
ISSUE_1.DRIVER_NO,
ISSUE_1.SHEET_ID,
ISSUE_2.NON_INVENT,
ISSUE_1.ORDER_STAT,
(CASE ISSUE_1.ORDER_STAT 
WHEN 'Canceled' THEN 'Cancelled'
WHEN 'Hold' THEN 'Hold'
WHEN 'WH-Hold' THEN 'Hold'
WHEN 'HoldConfirmed ' THEN 'Hold'
WHEN '%' THEN 'Cancelled'
WHEN 'Returned' THEN 'Returned'
WHEN 'Deliverd' THEN 'Success'
WHEN 'Paid' THEN 'Success'
WHEN 'Shipped' THEN 'Success'
WHEN 'Confirmed' THEN 'Unshipped'
WHEN 'Invoiced' THEN 'Unshipped'
WHEN 'PreConfirmed' THEN 'Unshipped'
WHEN 'Received' THEN 'Unshipped'
WHEN 'Shelved' THEN 'Unshipped'
WHEN 'Reschedule' THEN 'Unshipped'
WHEN 'ScheduleRq' THEN 'Cancelled'
WHEN 'Cancel Rq' THEN 'Cancelled'
WHEN 'CancelRq' THEN 'Cancelled'
ELSE 'Not Specified' END),
ITEMS.VENDOR,
SUPPLIERS.NAME_E,
SUPPLIERS.ACCNO,
ITEMS.HOLD_QTY,
datediff(ITEMS.LAST_ORDER_DATE , ITEMS.ADD_DT),
ITEMS.LAST_GRN_DATE,
datediff(ISSUE_1.VDATE, ITEMS.LAST_GRN_DATE),
datediff(DATE_SUB(ISSUE_1.VDATE, INTERVAL DAYOFMONTH(ISSUE_1.VDATE)-1 DAY), ITEMS.LAST_GRN_DATE),
ISSUE_1.CUSTNO

FROM

(select * from ISSUE_1 where ISSUE_1.VDATE > (select max(order_date) from aoi)) ISSUE_1
LEFT JOIN CASH_PAYMENTS ON ISSUE_1.PAYMENT = CASH_PAYMENTS.ID
LEFT JOIN CURRANCY ON ISSUE_1.CURR_ID = CURRANCY.ID
LEFT JOIN centers ON ISSUE_1.PCENTER = centers.ID
LEFT JOIN (select ISSUE_2.TOTAL charge, ISSUE_2.BILNO, ISSUE_2.CODE, ISSUE_2.PCENTER from (select * from ISSUE_1 where ISSUE_1.VDATE > (select max(order_date) from aoi)) ISSUE_1 LEFT JOIN ISSUE_2 ON ISSUE_1.BILNO=ISSUE_2.BILNO where PRODN in ('SHIPPINGBTQ','SHIPPINGSKU','SHIPPINGQATAR') group by ISSUE_2.BILNO) shipping ON ISSUE_1.BILNO=shipping.BILNO AND shipping.CODE =72
LEFT JOIN (select ISSUE_2.* from (select * from ISSUE_1 where ISSUE_1.VDATE > (select max(order_date) from aoi)) ISSUE_1 LEFT JOIN ISSUE_2 ON ISSUE_1.BILNO=ISSUE_2.BILNO where PRODN NOT IN ('SHIPPINGBTQ','SHIPPINGSKU','SHIPPINGQATAR') AND NON_INVENT = 4 ) ISSUE_2 ON ISSUE_1.BILNO = ISSUE_2.BILNO and ISSUE_2.CODE = 72 
LEFT JOIN (select ITEMS_ASSEM.PRODN boxno, sum(ITEMS_ASSEM.QTY*(CASE ITEMS_ASSEM.FREE_PRICE_SW WHEN 1 THEN 0 ELSE 1 END )*ITEMS.SAL_PRICE) cal_price from ITEMS left join ITEMS_ASSEM on ITEMS_ASSEM.PRODN1 = ITEMS.PRODN group by boxno) bp on bp.boxno = ISSUE_2.PRODN
LEFT JOIN ITEMS_ASSEM ON ITEMS_ASSEM.PRODN = ISSUE_2.PRODN 
LEFT JOIN MK_CELEB ON ISSUE_2.CATEGORY_ID = MK_CELEB.ID
LEFT JOIN ITEMS box_ITEMS on box_ITEMS.PRODN = ISSUE_2.PRODN 
LEFT JOIN ITEMS on ITEMS_ASSEM.PRODN1 = ITEMS.PRODN 
LEFT JOIN MK_BRANDS ON ITEMS.BRAND_ID = MK_BRANDS.ID
LEFT JOIN brand_purchase_type_master ON brand_purchase_type_master.brand_name=MK_BRANDS.NAME_E
LEFT JOIN PRODUCT_CAT1 ON ITEMS.CAT1 = PRODUCT_CAT1.ID
LEFT JOIN PRODUCT_CAT2 ON ITEMS.CAT2 = PRODUCT_CAT2.ID
LEFT JOIN PRODUCT_CAT3 ON ITEMS.CAT3 = PRODUCT_CAT3.ID
LEFT JOIN PRODUCT_CAT4 ON ITEMS.CAT4 = PRODUCT_CAT4.ID
LEFT JOIN SUPPLIERS ON ITEMS.VENDOR = SUPPLIERS.ID
LEFT JOIN ITEM_PRODS ON ITEMS.PRODN = ITEM_PRODS.PRODN AND ITEM_PRODS.PRODS = 1 
where ISSUE_2.NON_INVENT is NOT NULL GROUP BY ISSUE_1.BILNO, ISSUE_1.PCENTER, ITEMS.PRODN, ISSUE_2.ID)

UNION ALL

(SELECT 
ISSUE_1.VDATE,
extract(year_month from ISSUE_1.VDATE),
year(ISSUE_1.VDATE), 
week(ISSUE_1.VDATE), 
month(ISSUE_1.VDATE),
ISSUE_1.PCENTER BRANCHID,
ISSUE_2.PRODS,
ISSUE_1.BILNO cust,
ISSUE_1.DV_OS_TYPE,
(CASE ISSUE_1.PAYMENT WHEN 0 THEN 2 ELSE ISSUE_1.PAYMENT END ),
(CASE ISSUE_1.PAYMENT WHEN 0 THEN 'CASH on Delivery' ELSE CASH_PAYMENTS.NAME_E END),
ISSUE_2.ID LINEID,
ITEMS.PRODN SKU,
ITEMS.PRODNAME_E,
ITEMS.PAR_CODE,
(CASE ITEM_PRODS.COST WHEN 0 THEN ISSUE_2.ITEM_COST ELSE ITEM_PRODS.COST END ) ,
ITEMS.SAL_PRICE,
ITEMS.SAL_PRICE,
ISSUE_2.PRICE,
ITEMS.LAST_ORDER_PRICE,
PRODUCT_CAT1.NAME_E,
PRODUCT_CAT2.NAME_E,
PRODUCT_CAT3.NAME_E,
PRODUCT_CAT4.NAME_E,
ITEMS.BRAND_ID,
IFNULL(MK_BRANDS.NAME_E,ITEMS.BRAND_NAME),
brand_purchase_type_master.purchase_type,
NULL,
NULL,
NULL,
NULL,
ISSUE_2.DISC2,
ISSUE_2.QTY,
NULL,
ISSUE_2.PRICE*ISSUE_2.QTY,
shipping.charge,
ISSUE_1.DISCOUNT,
ISSUE_1.TOTAL,
(ISSUE_1.TOTAL - coalesce(ISSUE_1.DISCOUNT,0)),
ISSUE_1.CUSTOMER_NAME,
ISSUE_1.CUST_TEL,
ISSUE_1.ORDER_MOBILE,
(CASE ISSUE_1.ORDER_STAT WHEN 'Canceled'THEN 1 WHEN 'CancelRq' THEN 1 ELSE 0 END ),
(CASE ISSUE_1.ORDER_STAT WHEN 'Returned' THEN 1 ELSE 0 END ),
(CASE ISSUE_1.ORDER_STAT WHEN 'Deliverd' THEN 1 ELSE 0 END ),
ISSUE_1.ORDER_REF_NO,
MK_CELEB.NAME_E,
ISSUE_2.CATEGORY_ID,
ISSUE_2.CELEBRITY_PRICE,
0.1*ISSUE_2.PRICE*ISSUE_2.QTY,
ISSUE_1.ORDER_REGION,
ISSUE_1.ORDER_AREA,
centers.NAME_E,
(CASE ISSUE_2.CATEGORY_ID WHEN NULL THEN 0 ELSE 1 END ),
ISSUE_1.CURR_ID,
CURRANCY.NAME_E,
ISSUE_1.CURR_RATE,
CURRANCY.EX_RATE,
ISSUE_1.AWBSTATUS,
ISSUE_1.AWBNUMBER,
ISSUE_1.RETURN_AWBNUMBER,
ISSUE_1.DRIVER_NO,
ISSUE_1.SHEET_ID,
ISSUE_2.NON_INVENT,
ISSUE_1.ORDER_STAT,
(CASE ISSUE_1.ORDER_STAT 
WHEN 'Canceled' THEN 'Cancelled'
WHEN 'Hold' THEN 'Hold'
WHEN 'WH-Hold' THEN 'Hold'
WHEN 'HoldConfirmed ' THEN 'Hold'
WHEN '%' THEN 'Cancelled'
WHEN 'Returned' THEN 'Returned'
WHEN 'Deliverd' THEN 'Success'
WHEN 'Paid' THEN 'Success'
WHEN 'Shipped' THEN 'Success'
WHEN 'Confirmed' THEN 'Unshipped'
WHEN 'Invoiced' THEN 'Unshipped'
WHEN 'PreConfirmed' THEN 'Unshipped'
WHEN 'Received' THEN 'Unshipped'
WHEN 'Shelved' THEN 'Unshipped'
WHEN 'Reschedule' THEN 'Unshipped'
WHEN 'ScheduleRq' THEN 'Cancelled'
WHEN 'Cancel Rq' THEN 'Cancelled'
WHEN 'CancelRq' THEN 'Cancelled'
ELSE 'Not Specified' END),
ITEMS.VENDOR,
SUPPLIERS.NAME_E,
SUPPLIERS.ACCNO,
ITEMS.HOLD_QTY,
datediff(ITEMS.LAST_ORDER_DATE , ITEMS.ADD_DT),
ITEMS.LAST_GRN_DATE,
datediff(ISSUE_1.VDATE, ITEMS.LAST_GRN_DATE),
datediff(DATE_SUB(ISSUE_1.VDATE, INTERVAL DAYOFMONTH(ISSUE_1.VDATE)-1 DAY), ITEMS.LAST_GRN_DATE),
ISSUE_1.CUSTNO

FROM

(select * from ISSUE_1 where ISSUE_1.VDATE > (select max(order_date) from aoi) AND ISSUE_1.CODE = 72) ISSUE_1
LEFT JOIN (select ISSUE_2.* from (select * from ISSUE_1 where ISSUE_1.VDATE > (select max(order_date) from aoi)) ISSUE_1 LEFT JOIN ISSUE_2 ON ISSUE_1.BILNO=ISSUE_2.BILNO where PRODN NOT IN ('SHIPPINGBTQ','SHIPPINGSKU','SHIPPINGQATAR') AND NON_INVENT <> 4 ) ISSUE_2 ON ISSUE_1.BILNO = ISSUE_2.BILNO and ISSUE_2.CODE = 72 
LEFT JOIN CASH_PAYMENTS ON ISSUE_1.PAYMENT = CASH_PAYMENTS.ID 
LEFT JOIN CURRANCY ON ISSUE_1.CURR_ID = CURRANCY.ID 
LEFT JOIN centers ON ISSUE_1.PCENTER = centers.ID 
LEFT JOIN (select ISSUE_2.TOTAL charge, ISSUE_2.BILNO, ISSUE_2.CODE, ISSUE_2.PCENTER from (select * from ISSUE_1 where ISSUE_1.VDATE > (select max(order_date) from aoi)) ISSUE_1 LEFT JOIN ISSUE_2 ON ISSUE_1.BILNO=ISSUE_2.BILNO where PRODN in ('SHIPPINGBTQ','SHIPPINGSKU','SHIPPINGQATAR') group by ISSUE_2.BILNO) shipping ON ISSUE_1.BILNO=shipping.BILNO AND shipping.CODE =72 
LEFT JOIN ITEMS ON ITEMS.PRODN = ISSUE_2.PRODN 
LEFT JOIN MK_CELEB ON ISSUE_2.CATEGORY_ID = MK_CELEB.ID 
LEFT JOIN MK_BRANDS ON ITEMS.BRAND_ID = MK_BRANDS.ID 
LEFT JOIN brand_purchase_type_master ON brand_purchase_type_master.brand_name=MK_BRANDS.NAME_E
LEFT JOIN PRODUCT_CAT1 ON ITEMS.CAT1 = PRODUCT_CAT1.ID 
LEFT JOIN PRODUCT_CAT2 ON ITEMS.CAT2 = PRODUCT_CAT2.ID 
LEFT JOIN PRODUCT_CAT3 ON ITEMS.CAT3 = PRODUCT_CAT3.ID 
LEFT JOIN PRODUCT_CAT4 ON ITEMS.CAT4 = PRODUCT_CAT4.ID 
LEFT JOIN SUPPLIERS ON ITEMS.VENDOR = SUPPLIERS.ID 
LEFT JOIN ITEM_PRODS ON ITEMS.PRODN = ITEM_PRODS.PRODN AND ITEM_PRODS.PRODS = 1 
where ISSUE_2.NON_INVENT is NOT NULL GROUP BY ISSUE_1.BILNO, ISSUE_1.PCENTER, ITEMS.PRODN, ISSUE_2.ID);

drop table if exists status_history;
CREATE TABLE status_history (
  order_no bigint(20) NOT NULL,
  status_date timestamp(6) NULL DEFAULT NULL,
  status  varchar(30) DEFAULT NULL,
  delivery_date datetime(6) DEFAULT NULL,
  dispatch_date datetime(6) DEFAULT NULL,
  PRIMARY KEY (order_no)
);
insert into status_history (order_no, status_date, delivery_date, dispatch_date, status)
  select a.*, (select status from MAGENTO_ORDSTATUS_HIST where order_no= a.ono and status_date=a.sd) from 
  (select ORDER_NO ono, max(status_date) sd, max(if(status in ('Deliverd', 'Delivered'),status_date,NULL)) delivery_date, max(if(status = 'Shipped', status_date,NULL)) dispatch_date from MAGENTO_ORDSTATUS_HIST group by ORDER_NO) a where a.ono <> 0 group by a.ono;

update aoi a 
  left join status_history sh on a.order_number = sh.order_no
  set a.status = sh.status,
  a.status_group = (CASE sh.status 
                  WHEN 'Canceled' THEN 'Cancelled'
                  WHEN 'Hold' THEN 'Hold'
                  WHEN 'WH-Hold' THEN 'Hold'
                  WHEN 'HoldConfirmed ' THEN 'Hold'
                  WHEN '%' THEN 'Cancelled'
                  WHEN 'Returned' THEN 'Returned'
                  WHEN 'Deliverd' THEN 'Success'
                  WHEN 'Paid' THEN 'Success'
                  WHEN 'Shipped' THEN 'Success'
                  WHEN 'Confirmed' THEN 'Unshipped'
                  WHEN 'Invoiced' THEN 'Unshipped'
                  WHEN 'PreConfirmed' THEN 'Unshipped'
                  WHEN 'Received' THEN 'Unshipped'
                  WHEN 'Shelved' THEN 'Unshipped'
                  WHEN 'Reschedule' THEN 'Unshipped'
                  WHEN 'ScheduleRq' THEN 'Cancelled'
                  WHEN 'Cancel Rq' THEN 'Cancelled'
                  WHEN 'CancelRq' THEN 'Cancelled'
                  ELSE 'Not Specified' END),
  a.status_date = sh.status_date,
  a.delivery_date = sh.delivery_date,
  a.dispatch_date = sh.dispatch_date where a.order_date > subdate(current_date, interval 60 day);

update aoi a join c_activity c on a.order_number = c.order_number set a.is_first_order=1 where c.order_number is not NULL and  a.order_date > subdate(current_date, interval 15 day);

#Update NULL Celebrity Names if present in EMT DataSet.
update aoi bcm, (select celeb_emt.* from (select distinct celebrity_id,celebrity_name from events.events_header) celeb_emt left join (select distinct celebrity_id,celebrity_name from aoi) celeb_bicm on celeb_bicm.celebrity_id=celeb_emt.celebrity_id where celeb_emt.celebrity_name IS NOT NULL AND celeb_bicm.celebrity_name IS NULL) celeb_emt set bcm.celebrity_name=celeb_emt.celebrity_name
where bcm.celebrity_id=celeb_emt.celebrity_id AND bcm.celebrity_name IS NULL;

update aoi left join MK_CELEB on MK_CELEB.ID=aoi.celebrity_id
SET aoi.celebrity_name=MK_CELEB.NAME_E
where aoi.celebrity_name IS NULL;

