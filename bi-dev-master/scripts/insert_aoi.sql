insert into
  aoi (
  order_date, 
  yearmonth , 
  order_year , 
  order_week , 
  order_month , 
  branch_id , 
  store_id , 
  order_number , 
  device_type , 
  payment_method_id , 
  payment_method , 
  line_id , 
  sku , 
  sku_name , 
  sku_barcode , 
  item_cost , 
  new_price , 
  sales_price , 
  unit_price , 
  last_order_price , 
  category1 , 
  category2, 
  category3 , 
  category4 , 
  brand_id , 
  brand_name , 
  box_id , 
  box_price , 
  box_discount , 
  box_name , 
  discount , 
  quantity , 
  box_quantity , 
  item_total , 
  shipping_charge , 
  order_discount , 
  order_total , 
  order_gmv , 
  customer_name , 
  customer_telephone , 
  order_mobile , 
  is_cancelled , 
  is_returned , 
  is_b_fulfilled , 
  order_ref_no , 
  celebrity_name , 
  celebrity_id , 
  celebrity_price , 
  celebrity_commission , 
  order_region , 
  city , 
  country , 
  is_celebrity , 
  currency_id , 
  currency , 
  currency_rate , 
  ex_rate , 
  awbstatus , 
  awbnumber , 
  return_awbnumber , 
  driver_id , 
  runsheet_no , 
  non_inventory , 
  status , 
  status_group,
  supplier_id , 
  supplier_name , 
  supplier_acc_no , 
  soh , 
  sales_period , 
  last_grn_date,
  ageing,
  ageing_month

)

(SELECT 
ISSUE_1.VDATE,
year(ISSUE_1.VDATE)+month(ISSUE_1.VDATE) * 10000,
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
items.PRODN SKU,
items.PRODNAME_E,
items.PAR_CODE,
(CASE item_prods.COST WHEN 0 THEN ISSUE_2.ITEM_COST ELSE item_prods.COST END ) ,
items.SAL_PRICE,
(items.SAL_PRICE*ISSUE_2.PRICE/(bp.cal_price))*(CASE ITEMS_ASSEM.FREE_PRICE_SW WHEN 1 THEN 0 ELSE 1 END ),
items.SAL_PRICE,
items.LAST_ORDER_PRICE,
PRODUCT_CAT1.NAME_E,
PRODUCT_CAT2.NAME_E,
PRODUCT_CAT3.NAME_E,
PRODUCT_CAT4.NAME_E,
items.BRAND_ID,
MK_BRANDS.NAME_E,
ISSUE_2.PRODN,
box_items.SAL_PRICE,
ISSUE_2.DISC2,
box_items.PRODNAME_E,
(items.SAL_PRICE - (items.SAL_PRICE*ISSUE_2.PRICE/(bp.cal_price))*(CASE ITEMS_ASSEM.FREE_PRICE_SW WHEN 1 THEN 0 ELSE 1 END ) )*100/items.SAL_PRICE,
(ISSUE_2.QTY * ITEMS_ASSEM.QTY),
ISSUE_2.QTY,
(items.SAL_PRICE*ISSUE_2.PRICE/(bp.cal_price))*(ITEMS_ASSEM.QTY*ISSUE_2.QTY)*(CASE ITEMS_ASSEM.FREE_PRICE_SW WHEN 1 THEN 0 ELSE 1 END ),
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
0.1*(items.SAL_PRICE*ISSUE_2.TOTAL/(ISSUE_2.QTY*bp.cal_price))*(ITEMS_ASSEM.QTY*ISSUE_2.QTY)*(CASE ITEMS_ASSEM.FREE_PRICE_SW WHEN 1 THEN 0 ELSE 1 END ),
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
(CASE  ISSUE_1.ORDER_STAT 
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
items.VENDOR,
SUPPLIERS.NAME_E,
SUPPLIERS.ACCNO,
items.HOLD_QTY,
datediff(items.LAST_ORDER_DATE , items.ADD_DT),
items.LAST_GRN_DATE,
datediff(ISSUE_1.VDATE, items.LAST_GRN_DATE),
datediff(DATE_SUB(ISSUE_1.VDATE, INTERVAL DAYOFMONTH(ISSUE_1.VDATE)-1 DAY), items.LAST_GRN_DATE)

FROM

ISSUE_1 
LEFT JOIN CASH_PAYMENTS ON ISSUE_1.PAYMENT = CASH_PAYMENTS.ID
LEFT JOIN CURRANCY ON ISSUE_1.CURR_ID = CURRANCY.ID
LEFT JOIN centers ON ISSUE_1.PCENTER = centers.ID
LEFT JOIN (select TOTAL charge, BILNO, CODE, PCENTER from ISSUE_2 where PRODN in ('SHIPPINGBTQ','SHIPPINGSKU','SHIPPINGQATAR') group by BILNO) shipping ON ISSUE_1.BILNO=shipping.BILNO
AND ISSUE_1.code=shipping.CODE
AND ISSUE_1.pcenter=shipping.PCENTER
AND ISSUE_1.code =72,
ISSUE_2 
LEFT JOIN (select ITEMS_ASSEM.PRODN boxno, sum(ITEMS_ASSEM.QTY*(CASE ITEMS_ASSEM.FREE_PRICE_SW WHEN 1 THEN 0 ELSE 1 END )*items.SAL_PRICE) cal_price from items left join ITEMS_ASSEM on ITEMS_ASSEM.PRODN1 = items.PRODN group by boxno) bp on bp.boxno = ISSUE_2.PRODN
LEFT JOIN MK_CELEB ON ISSUE_2.CATEGORY_ID = MK_CELEB.ID,
items box_items,
items
LEFT JOIN MK_BRANDS  ON items.BRAND_ID = MK_BRANDS.ID
LEFT JOIN PRODUCT_CAT1 ON items.CAT1 = PRODUCT_CAT1.ID
LEFT JOIN PRODUCT_CAT2 ON items.CAT2 = PRODUCT_CAT2.ID
LEFT JOIN PRODUCT_CAT3 ON items.CAT3 = PRODUCT_CAT3.ID
LEFT JOIN PRODUCT_CAT4 ON items.CAT4 = PRODUCT_CAT4.ID
LEFT JOIN SUPPLIERS ON items.VENDOR = SUPPLIERS.ID
LEFT JOIN item_prods ON items.PRODN = item_prods.PRODN AND item_prods.PRODS = 1,
ITEMS_ASSEM

WHERE ISSUE_1.BILNO = ISSUE_2.BILNO
AND ISSUE_1.CODE = ISSUE_2.CODE
AND ISSUE_1.PCENTER = ISSUE_2.PCENTER
AND ISSUE_1.CODE = 72
AND ITEMS_ASSEM.PRODN = ISSUE_2.PRODN
AND ITEMS_ASSEM.prodn1 = items.PRODN
AND ISSUE_2.PRODN NOT IN ('SHIPPINGBTQ','SHIPPINGSKU','SHIPPINGQATAR')
AND box_items.PRODN = ISSUE_2.PRODN
AND box_items.NON_INVENTORY_ITEM = 4)

UNION ALL

(SELECT 
ISSUE_1.VDATE,
year(ISSUE_1.VDATE)+month(ISSUE_1.VDATE) * 10000,
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
items.PRODN SKU,
items.PRODNAME_E,
items.PAR_CODE,
(CASE item_prods.COST WHEN 0 THEN ISSUE_2.ITEM_COST ELSE item_prods.COST END ) ,
items.SAL_PRICE,
items.SAL_PRICE,
ISSUE_2.PRICE,
items.LAST_ORDER_PRICE,
PRODUCT_CAT1.NAME_E,
PRODUCT_CAT2.NAME_E,
PRODUCT_CAT3.NAME_E,
PRODUCT_CAT4.NAME_E,
items.BRAND_ID,
MK_BRANDS.NAME_E,
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
(CASE  ISSUE_1.ORDER_STAT 
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
items.VENDOR,
SUPPLIERS.NAME_E,
SUPPLIERS.ACCNO,
items.HOLD_QTY,
datediff(items.LAST_ORDER_DATE , items.ADD_DT),
items.LAST_GRN_DATE,
datediff(ISSUE_1.VDATE, items.LAST_GRN_DATE),
datediff(DATE_SUB(ISSUE_1.VDATE, INTERVAL DAYOFMONTH(ISSUE_1.VDATE)-1 DAY), items.LAST_GRN_DATE)

FROM

ISSUE_1 
LEFT JOIN CASH_PAYMENTS ON ISSUE_1.PAYMENT = CASH_PAYMENTS.ID
LEFT JOIN CURRANCY ON ISSUE_1.CURR_ID = CURRANCY.ID
LEFT JOIN centers ON ISSUE_1.PCENTER = centers.ID
LEFT JOIN (select TOTAL charge, BILNO, CODE, PCENTER from ISSUE_2 where PRODN in ('SHIPPINGBTQ','SHIPPINGSKU','SHIPPINGQATAR') group by BILNO) shipping ON ISSUE_1.BILNO=shipping.BILNO
And ISSUE_1.code=shipping.CODE
And ISSUE_1.pcenter=shipping.PCENTER
And ISSUE_1.code =72,
ISSUE_2 
LEFT JOIN MK_CELEB ON ISSUE_2.CATEGORY_ID = MK_CELEB.ID,
items
LEFT JOIN MK_BRANDS  ON items.BRAND_ID = MK_BRANDS.ID
LEFT JOIN PRODUCT_CAT1 ON items.CAT1 = PRODUCT_CAT1.ID
LEFT JOIN PRODUCT_CAT2 ON items.CAT2 = PRODUCT_CAT2.ID
LEFT JOIN PRODUCT_CAT3 ON items.CAT3 = PRODUCT_CAT3.ID
LEFT JOIN PRODUCT_CAT4 ON items.CAT4 = PRODUCT_CAT4.ID
LEFT JOIN SUPPLIERS ON items.VENDOR = SUPPLIERS.ID
LEFT JOIN item_prods ON items.PRODN = item_prods.PRODN AND item_prods.PRODS = 1
WHERE ISSUE_1.BILNO = ISSUE_2.BILNO
  AND ISSUE_1.CODE = ISSUE_2.CODE
  AND ISSUE_1.PCENTER = ISSUE_2.PCENTER
  AND ISSUE_1.CODE = 72
  AND items.PRODN = ISSUE_2.PRODN
  AND ISSUE_2.PRODN NOT IN ('SHIPPINGBTQ','SHIPPINGSKU','SHIPPINGQATAR')
  AND items.NON_INVENTORY_ITEM <> 4);