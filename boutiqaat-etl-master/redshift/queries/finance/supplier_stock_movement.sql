BEGIN;
DROP TABLE IF EXISTS analytics.supplier_stock_movement;

select * into analytics.supplier_stock_movement
FROM
(select 
  ile.item_no as sku,
  i.description as description,
  v.vendor_no as supplier_code,
  v.vendor_name as supplier,
  ile.entry_type,
  ile.document_type,
  ile.document_no,
  ile.document_line_no,
  ile.document_date,
  ile.quantity,
  i.unit_cost as unit_cost,
  ile.foc,
  ile.inventory_type,
  ile.location_code,
  ile.bin_type_code,
  loc.name as location_name,
  sil.sales_order_no,
  lower(od.order_category) as order_category,
  lower(od.order_type) as order_type,
  lower(od.order_status) as order_status,
  ile.closed,
  ile.lot_no,
  ile.bin_code,
  ho.hold_quantity,
  case when od.order_status in ('Shipped','CLOSE', 'Delivered') then True else False end as is_net,
  CAST(i.unit_cost as DECIMAL(10,3)) * COALESCE(ile.quantity,0) as total_cost
from 
(
select 
	COALESCE(ile.item_no, we.item_no) as item_no,
	COALESCE(ile.entry_type, we.entry_type) as entry_type,
	COALESCE(ile.document_type, we.whse_document_type) as document_type,
	COALESCE(ile.document_no, we.whse_document_no) as document_no,
	COALESCE(ile.document_line_no, we.whse_document_line_no) as document_line_no,
	COALESCE(DATE(ile.posting_date), DATE(we.registering_date)) as document_date,
	COALESCE(ile.quantity,we.quantity) as quantity,
	ile.foc,
	ile.inventory_type,
	COALESCE(ile.location_code, we.location_code) as location_code,
	COALESCE(we.bin_type_code, NULL) as bin_type_code,
	we.closed,
	COALESCE(ile.lot_no, we.lot_no) as lot_no,
	we.bin_code
from nav.item_ledger_entry ile
full outer join nav.warehouse_entry we
ON we.item_no=ile.item_no and we.whse_document_no=ile.document_no and we.whse_document_line_no=ile.document_line_no AND DATE(ile.posting_date)=DATE(we.registering_date)
) ile
left join nav.item i ON i.no=ile.item_no
left join (select iv.item_no as sku, iv.vendor_no as vendor_no, v.name as vendor_name 
			from nav.item_vendor iv left join nav.vendor v ON v.no=iv.vendor_no) v ON v.sku=ile.item_no
left join nav.sales_invoice_line sil ON sil.no=ile.item_no and sil.document_no=ile.document_no
left join aoi.sales_order_items od ON od.item_id=sil.item_id and od.order_number=sil.sales_order_no
left join nav.location loc on loc.code=ile.location_code
left join (select isl.sku, sum(isl.quantity) hold_quantity 
			from ofs.inbound_sales_line isl 
			join ofs.hold_orders ho 
			ON ho.web_order_no=isl.web_order_no and ho.item_id=isl.item_id 
			group by 1) ho ON ile.item_no=ho.sku);

COMMIT;

/*

select * into analytics.supplier_stock_movement from (
select
    supplier_code,
    supplier,
    sku,
    description,
    -- grn
    MAX(unit_cost) as unit_cost,
    COUNT(DISTINCT CASE WHEN entry_type=0 and document_type=5 THEN document_no ELSE NULL END) as grn_no,
    SUM(CASE WHEN entry_type=0 and document_type=5 THEN quantity ELSE 0 END) as grn_qty,
    SUM(CASE WHEN entry_type=0 and document_type=5 and inventory_type=3 THEN quantity ELSE 0 END) as grn_foc,
    CAST(MAX(unit_cost) as DECIMAL(10,3)) * SUM(CASE WHEN entry_type=0 and document_type=5 THEN quantity ELSE 0 END) as grn_total_cost,
    -- purch return pr
    COUNT(DISTINCT CASE WHEN entry_type=0 and document_type=7 THEN document_no ELSE NULL END) as pr_no,
    SUM(CASE WHEN entry_type=0 and document_type=7 THEN quantity ELSE 0 END) as pr_qty,
    SUM(CASE WHEN entry_type=0 and document_type=7 and inventory_type=3 THEN quantity ELSE 0 END) as pr_foc,
    CAST(MAX(unit_cost) as DECIMAL(10,3)) * SUM(CASE WHEN entry_type=0 and document_type=7 THEN quantity ELSE 0 END) as pr_total_cost,
    -- net sales 
    COUNT(DISTINCT CASE 
                      WHEN entry_type=1 
                      and document_type=2 
                      and is_net THEN document_no 
                      ELSE NULL END) as net_sales_no,
    ABS(SUM(CASE 
              WHEN entry_type=1 
              and document_type=2 
              and is_net
              and lower(order_category) != 'celebrity' THEN quantity  
              ELSE 0 END)) as net_sales_qty,
    -- celebrity gifting
    ABS(SUM(CASE 
              WHEN entry_type=1 
              and document_type=2
              and lower(order_category)='celebrity' THEN quantity 
              ELSE 0 END)) as celeb_gift_qty,
    -- replacement quantity
    ABS(SUM(CASE 
              WHEN entry_type=1 
              and document_type=2
              and lower(order_type)='exchange' THEN quantity 
              ELSE 0 END)) as replacement_qty,
    -- Marketing gifting
    ABS(SUM(CASE 
              WHEN entry_type=1 
              and document_type=2
              and lower(order_category)='vip gifting' THEN quantity 
              ELSE 0 END)) as mktg_gift_qty,
    -- customer gifting
    ABS(SUM(CASE 
              WHEN entry_type=1 
              and document_type=2
              and inventory_type=3 THEN quantity 
              ELSE 0 END)) as cutomer_gift_qty,
    -- Offline sales
    ABS(SUM(CASE 
              WHEN entry_type=1 
              and document_type=2
              and location_code='KWIMISHREF' THEN quantity 
              ELSE 0 END)) as offline_sales_qty,
    -- stock adjustment
    ABS(SUM(CASE 
              WHEN is_net is False and closed=0 and bin_code='STAGEMOVEBIN' THEN quantity 
              ELSE 0 END)) as stock_adj_qty,
    -- total quantity
    ABS(SUM(quantity)) as total_qty,
    -- showroom quantity
    ABS(SUM(CASE 
              WHEN location_code='KWISHOWROO' THEN quantity 
              ELSE 0 END)) as showroom_qty,
    -- content quantity
    ABS(SUM(CASE 
              WHEN location_code='KWICONTENT' THEN quantity 
              ELSE 0 END)) as content_qty,
    -- damaged quantity
    ABS(SUM(CASE 
              WHEN location_code='KWIDMG' THEN quantity 
              ELSE 0 END)) as damage_qty,
    -- Management quantity
    ABS(SUM(CASE 
              WHEN location_code='KWIMGMT' THEN quantity 
              ELSE 0 END)) as mgmt_qty,
    -- hold quantity
    MAX(hold_quantity) as hold_quantity,
    -- Main warehouse quantity
    SUM(CASE 
              WHEN location_code like 'KWI0%' THEN quantity 
              ELSE 0 END) as main_wh_qty,
    -- main warehouse quantity cost
    CAST(MAX(unit_cost) as DECIMAL(10,3)) * SUM(CASE 
              WHEN location_code like 'KWI0%' THEN quantity 
              ELSE 0 END) as main_wh_total_cost
FROM (
select 
  ile.item_no as sku,
  i.description as description,
  v.vendor_no as supplier_code,
  v.vendor_name as supplier,
  ile.entry_type,
  ile.document_type,
  ile.document_no,
  ile.quantity,
  i.unit_cost as unit_cost,
  ile.foc,
  ile.inventory_type,
  ile.location_code,
  loc.name,
  sil.sales_order_no,
  od.order_category,
  od.order_type,
  od.order_status,
  we.closed,
  we.lot_no,
  we.bin_code,
  ho.hold_quantity,
  case when od.order_status in ('Shipped','CLOSE', 'Delivered') then True else False end as is_net
from nav.item_ledger_entry ile
left join nav.item i ON i.no=ile.item_no
left join (select iv.item_no as sku, iv.vendor_no as vendor_no, v.name as vendor_name 
			from nav.item_vendor iv left join nav.vendor v ON v.no=iv.vendor_no) v ON v.sku=ile.item_no
left join nav.sales_invoice_line sil ON sil.no=ile.item_no and sil.document_no=ile.document_no
left join aoi.sales_order_items od ON od.item_id=sil.item_id and od.order_number=sil.sales_order_no
left join nav.location loc on loc.code=ile.location_code
left join nav.warehouse_entry we ON we.item_no=ile.item_no and we.whse_document_no=ile.document_no and we.source_line_no=ile.document_line_no
left join (select isl.sku, sum(isl.quantity) hold_quantity 
			from ofs.inbound_sales_line isl 
			join ofs.hold_orders ho 
			ON ho.web_order_no=isl.web_order_no and ho.item_id=isl.item_id 
			group by 1) ho ON ile.item_no=ho.sku
)
group by 1,2,3,4 */