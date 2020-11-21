CREATE OR REPLACE VIEW analytics.high_cover_beauty as
SELECT 
hc.sku,
hc.barcode,
hc.sku_name,
hc.brand,
hc.category1,
hc.department,
hc.vendor_code,
hc.supplier,
hc.first_grn_date,
hc.first_order_date,
hc.soh,
hc.open_po_qty,
hc.sold_qty_14days,
hc.sold_qty_30_days,
hc.sold_qty_60_days,
hc.sold_qty_90_days,
hc.sold_qty_180_days,
hc.lifetime_sold_qty,
hc.stock_cover_60days_sale_basis,
hc.stock_cover_flag,
hc.inventory_category,
hc.high_cover_type,
hc.Excess_soh,
hc.Excess_PO_QTY,
hc.RTV_Excess_soh,
hc.Action_on_Open_PO,
(case 
when hc.Excess_soh =0 and hc.Excess_PO_QTY = 0 then 'No Action'
when hc.Excess_soh > 0 and hc.Excess_PO_QTY = 0 then 'RTV Excess SOH'
when hc.Excess_soh = 0 and hc.Excess_PO_QTY > 0 then 'Close Excess PO QTY'
when hc.Excess_soh > 0 and hc.Excess_PO_QTY > 0 then 'RTV Excess SOH & Close All PO'
else 'No Action'
end)Actionable
FROM
(select 
sku,
barcode,
sku_name,
brand,
category1,
department,
vendor_code,
supplier,
first_grn_date,
first_order_date,
soh,
open_po_qty,
sold_qty_14days,
sold_qty_30_days,
sold_qty_60_days,
sold_qty_90_days,
sold_qty_180_days,
lifetime_sold_qty,
cast(stock_cover_60days_sale_basis as decimal(8,2)) stock_cover_60days_sale_basis,
stock_cover_flag,
inventory_category,
(case 
when stock_cover_60days_sale_basis::decimal(8,2)>0 and stock_cover_60days_sale_basis::decimal(8,2)<=3 THEN 'Cover Between 0 and 3 months' 
when stock_cover_60days_sale_basis::decimal(8,2)>3 and stock_cover_60days_sale_basis::decimal(8,2)<=4 THEN 'Cover Between 3 and 4 months'
when stock_cover_60days_sale_basis::decimal(8,2)>4 and stock_cover_60days_sale_basis::decimal(8,2)<=5 THEN 'Cover Between 4 and 5 months' 
when stock_cover_60days_sale_basis::decimal(8,2)>5 and stock_cover_60days_sale_basis::decimal(8,2)<=6 THEN 'Cover Between 5 and 6 months'
when stock_cover_60days_sale_basis::decimal(8,2)>6 and stock_cover_60days_sale_basis::decimal(8,2)<=7 THEN 'Cover Between 6 and 7 months' 
else 'Cover >7 months' 
end ) high_cover_type,
ROUND(GREATEST(0,(soh*(1-(4/stock_cover_60days_sale_basis::decimal(8,2)))))) Excess_soh,
ROUND(least(open_po_qty, ((soh+open_po_qty)*(1-(4/stock_cover_60days_including_open_po::decimal(8,2)))))) Excess_PO_QTY,
(case 
when stock_cover_60days_sale_basis::decimal(8,2)>4 then 'RTV of Excess QTY' else 'No Action'
end)RTV_Excess_soh,
(case 
when stock_cover_60days_including_open_po::decimal(8,2)>4 then 'Cancel_Excess_PO_QTY' else 'No Action'
end) Action_on_Open_PO
FROM aoi.inventory_health 
where stock_cover_flag in ('High_Cover', 'Normal_Cover') 
and ((stock_cover_60days_sale_basis::decimal(8,2))>4 or (stock_cover_60days_including_open_po::decimal(8,2))>4)
and (department = 'Beauty' or department is null) 
group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26) hc
where Excess_soh>0 or Excess_PO_QTY>0
group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27
WITH NO SCHEMA BINDING;