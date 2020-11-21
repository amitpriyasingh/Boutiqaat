SELECT
	distinct i.[No_]  sku, 
	REPLACE(REPLACE(REPLACE(REPLACE(i.[Description], CHAR(13), ''), CHAR(10), ''), CHAR(9), ''),'\"', '') as sku_name, 
	iam.[Description] as brand,
	i.[Item Category Code] as category1, 
	REPLACE(REPLACE(REPLACE(REPLACE(i.[Vendor Item No_], CHAR(13), ''), CHAR(10), ''), CHAR(9), ''),'\"', '') as vendor_item_no,
	COALESCE(item_stock.putpick_qty,0)+COALESCE(stock_details.qty_movement,0)+COALESCE(item_stock.rtrn_qc_pass,0)+COALESCE(item_stock.stgmv_qty,0) as total_sellable_qty,
	((COALESCE (item_stock.non_sellable_online_kwi01,0)-COALESCE(stgmv_qty,0)) + 
		COALESCE(ile.mishref_qty,0) + 
		COALESCE(wh_entry.intransit_sys_transfer_trnc_qty,0) + 
		COALESCE (ile.content_abzak_qty,0) + 
		COALESCE (ile.showroom_6th_floor_qty,0) + 
		COALESCE (ile.qatar_exhibition_qty,0) + 
		COALESCE (wh_entry.old_return_solv_qty,0) + 
		COALESCE (ile.mgmt_qty,0) + 
		COALESCE (ile.kwpurch_qty,0) + 
		COALESCE (ile.showroom_m1_floor_qty,0) + 
		COALESCE (ile.abrar_qty,0) + 
		COALESCE (ile.marketing_gift_qty,0) + 
		COALESCE (ile.new_showroom_qty,0) +
		COALESCE(wh_entry.intransit_sys_transfer_trnc_qty,0) +
		COALESCE (ile.tv_mashour_qty,0) +
		COALESCE (ile.other_offline_qty,0) +
		COALESCE (ile.designer_hamdan_qty,0) +
		COALESCE (ile.sample_qty,0)+
		COALESCE (ile.other_qty,0)
	) as toal_nav_non_sellable,
	(COALESCE(item_stock.putpick_qty,0)+
		COALESCE(stock_details.qty_movement,0)+
		COALESCE(item_stock.rtrn_qc_pass,0)+
		COALESCE(item_stock.stgmv_qty,0)+
		(COALESCE (item_stock.non_sellable_online_kwi01,0)-COALESCE(stgmv_qty,0)) + 
		COALESCE(ile.mishref_qty,0) + 
		COALESCE(wh_entry.intransit_sys_transfer_trnc_qty,0) + 
		COALESCE (ile.content_abzak_qty,0) + 
		COALESCE (ile.showroom_6th_floor_qty,0) + 
		COALESCE (ile.qatar_exhibition_qty,0) + 
		COALESCE (wh_entry.old_return_solv_qty,0) + 
		COALESCE (ile.mgmt_qty,0) + 
		COALESCE (ile.kwpurch_qty,0) + 
		COALESCE (ile.showroom_m1_floor_qty,0) + 
		COALESCE (ile.abrar_qty,0) + 
		COALESCE (ile.marketing_gift_qty,0) + 
		COALESCE (ile.new_showroom_qty,0) +
		COALESCE(wh_entry.intransit_sys_transfer_trnc_qty,0) +
		COALESCE (ile.tv_mashour_qty,0) +
		COALESCE (ile.other_offline_qty,0) +
		COALESCE (ile.designer_hamdan_qty,0) +
		COALESCE (ile.sample_qty,0)+
		COALESCE (ile.other_qty,0)
	) as SOH,
	item_stock.stock_refreshed_datetime
from (select * from [Boutiqaat_Live].[dbo].[Boutiqaat Kuwait\$Item] where [Item Category Code] in ('MEN','WOMEN')) i 
left join [Boutiqaat_Live].[dbo].[Boutiqaat Kuwait\$Item Attribute Master] iam WITH (NOLOCK) on iam.[Code] = i.[Brand]
left join 
(
	select 
		[Item No] as sku,
		max([Insert DateTime]) as stock_refreshed_datetime, 
		sum(case when [Bin Type]='PUTPICK' then [QTY] else 0 end)   as putpick_qty,
		sum(case when [Bin No] IN ('RPASS001','KWI01-IB01-QC01-01A1') then [QTY] else 0 end)  as rtrn_qc_pass,
		sum(case when [Bin No] = 'STAGEMOVEBIN' then [QTY] else 0 end) as stgmv_qty,
		sum(case when [Bin Type] not in ('PUTPICK','RECEIVE') and [Bin No] <> 'RPASS001' then [QTY] else 0 end)   as non_sellable_online_kwi01
	from [Boutiqaat_Live].[dbo].[Boutiqaat Kuwait\$Item Stock] group by [Item No]
) item_stock on item_stock.sku = i.[No_]
left join 
(
	SELECT 
		sd.[Item No] sku, 
		MaxEntry as  max_entry_no, 
		[Qty in Stock] nav2crs_total, 
		[Warehouse Entry] wh_entry, 
		[Warehouse JN] wh_jn_line,    
		[Warehouse Activity Line] wh_activity_line,    
		[Warehouse Activity Line 2] wh_activity_line2,
		[Movement Journal Line] qty_movement,
		[Reserved Quantity] reserved_qty 
	FROM 
	(
		SELECT 
			[Item No] SKU, 
			max([Entry No]) as MaxEntry 
		FROM [Boutiqaat_Live].[dbo].[Boutiqaat Kuwait\$Stock Details] WITH (NOLOCK) 
		where [Stock Sync NewStack]=1 
		GROUP BY [Item No]
	) sd_last_entry 
	INNER JOIN [Boutiqaat_Live].[dbo].[Boutiqaat Kuwait\$Stock Details] sd WITH (NOLOCK) ON sd.[Item No] = sd_last_entry.SKU 
	AND sd.[Entry No] = sd_last_entry.MaxEntry 
) as stock_details on stock_details.sku = i.[No_]
left join 
(
	select 
		[Item No_] as sku,
		sum(case when [Location Code] = 'KWIDMG' then [Quantity] else 0 end) as damaged_qty,
		sum(case when [Location Code] = 'KWIEXPR' then [Quantity] else 0 end) as exp_qty,
		sum(case when [Location Code] = 'KWIMISHREF' then [Quantity] else 0 end) as mishref_qty,
		sum(case when [Location Code] = 'KWIPHOTO' then [Quantity] else 0 end) as content_abzak_qty,
		sum(case when [Location Code] = 'KWISHOWROO' then [Quantity] else 0 end) as showroom_6th_floor_qty,
		sum(case when [Location Code] = 'KWIMNSHOWR' then [Quantity] else 0 end) as showroom_m1_floor_qty,
		sum(case when [Location Code] = 'QATAREXPO' then [Quantity] else 0 end) as qatar_exhibition_qty,
		sum(case when [Location Code] = 'KWIMGMT' then [Quantity] else 0 end) as mgmt_qty,
		sum(case when [Location Code] = 'KWIPURCH' then [Quantity] else 0 end) as kwpurch_qty,
		sum(case when [Location Code] = 'KWIABRAR' then [Quantity] else 0 end) as abrar_qty,
		sum(case when [Location Code] = 'KWIMRKTEXP' then [Quantity] else 0 end) as marketing_gift_qty,
		sum(case when [Location Code] = 'KWIWMSHOWR' then [Quantity] else 0 end) as new_showroom_qty,
		sum(case when [Location Code] = 'KWICONTENT' then [Quantity] else 0 end) as tv_mashour_qty,
		sum(case when [Location Code] = 'OFFLINE' then [Quantity] else 0 end) as other_offline_qty,
		sum(case when [Location Code] = 'KWIHO' then [Quantity] else 0 end) as designer_hamdan_qty,
		sum(case when [Location Code] = 'KWSAMPLE' then [Quantity] else 0 end) as sample_qty,
		sum(case when [Location Code] in ('KWI01SHM','KWI02','KWI03','KWI04','KWIPKG01') then [Quantity] else 0 end) as other_qty
	from [Boutiqaat_Live].[dbo].[Boutiqaat Kuwait\$Item Ledger Entry] 
	group by [Item No_]
) ile on ile.sku = i.[No_]
left join 
(
	select 
		[Item No_] as sku,
		sum(case when [Location Code]='DXBSAMPLE' and [Bin Type Code] in ('QC','RECEIVE') then [Quantity] else 0 end) as intransit_sys_transfer_trnc_qty,
		sum(case when [Location Code]='BOR' and [Bin Type Code] in ('QC','RECEIVE') then [Quantity] else 0 end) as old_return_solv_qty,
		sum(case when [Location Code]='KWSAMPLE' and [Bin Type Code] in ('QC','RECEIVE') then [Quantity] else 0 end) as sample_qty
	from [Boutiqaat_Live].[dbo].[Boutiqaat Kuwait\$Warehouse Entry] 
	group by [Item No_]
) wh_entry on wh_entry.sku = i.[No_]
left join 
(
	select 
		[Item No] as sku, 
		sum([QTY]) as qty_kwi 
	from [Boutiqaat_Live].[dbo].[Boutiqaat Kuwait\$Item Stock] 
where [Bin Type] not in ('PUTPICK','RECEIVE') and [Bin No] not in ('RPASS001','STAGEMOVEBIN') and [Location] = 'KWI01'
group by [Item No]
) itemstock_kwi on itemstock_kwi.sku = i.[No_]
left join 
(
	select 
		item.sku,
		sum(ile.qty) as qty_other 
	from 
	(
		select 
			[Item No] as sku, 
			[Location] as location
		from [Boutiqaat_Live].[dbo].[Boutiqaat Kuwait\$Item Stock]
		where [Bin Type] not in ('PUTPICK','RECEIVE') and [Bin No] not in ('RPASS001','STAGEMOVEBIN') and [Location] <> 'KWI01'
		group by [Item No],[Location]
	) item
	inner join 
	(
		select 
			[Item No_] as sku,
			[Location Code] as location, 
			sum([Quantity]) as qty  
		from [Boutiqaat_Live].[dbo].[Boutiqaat Kuwait\$Item Ledger Entry] 
		where [Location Code] <> 'KWI01'  
		group by [Item No_],[Location Code]
	) ile on ile.sku = item.sku and ile.location = item.location
	group by item.sku
) itemstock_other on itemstock_other.sku = i.[No_]
WHERE \$CONDITIONS

