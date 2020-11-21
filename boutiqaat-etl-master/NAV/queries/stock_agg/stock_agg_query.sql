SELECT * FROM (
SELECT 
    i.[No_] sku,
    COALESCE(item_stock.putpick_qty,0) as putpick_qty,
    COALESCE(stock_details.qty_movement,0) as qty_movement,
    COALESCE(item_stock.rtrn_qc_pass,0) as qty_return_qc_pass_and_grn_pending_putaway,
    COALESCE(item_stock.stgmv_qty,0) as qty_stagemovebin,
    COALESCE(item_stock.putpick_qty,0)+COALESCE(stock_details.qty_movement,0)+COALESCE(item_stock.rtrn_qc_pass,0)+(COALESCE(item_stock.stgmv_qty,0)) as total_sellable_qty,
    COALESCE(stock_details.nav2crs_total,0) as nav2crs_total,
    COALESCE(stock_details.wh_entry,0) as wh_entry,
    COALESCE(stock_details.wh_jn_line,0) as wh_jn_line,
    COALESCE(stock_details.wh_activity_line,0) as wh_activity_line,
    COALESCE(item_stock.ccstage_qty,0) as non_sellable_qty_ccstagebin,
    COALESCE(ile.damaged_qty,0) as non_sellable_qty_damaged_inventory,
    COALESCE(ile.exp_qty,0) as non_sellable_qty_exp_inventory,
    COALESCE(stock_details.reserved_qty,0) as warehouse_reserved_qty,
    (COALESCE (item_stock.non_sellable_online_kwi01,0)- COALESCE(stgmv_qty,0)) as non_sellable_online_store_qty,
    COALESCE(ile.mishref_qty,0) as non_sellable_mishref_qty,
    COALESCE(wh_entry.intransit_sys_transfer_trnc_qty,0) as non_sellable_intransit_sys_qty,
    COALESCE (ile.content_abzak_qty,0) as non_sellable_content_abzak_qty,
    COALESCE (ile.showroom_6th_floor_qty,0) as non_sellable_showroom_6th_floor_qty,
    COALESCE (ile.qatar_exhibition_qty,0) as non_sellable_qatar_exhibition_qty,
    COALESCE (wh_entry.old_return_solv_qty,0) as non_sellable_old_return_solv_qty,
    COALESCE (ile.mgmt_qty,0) as non_sellable_mgmt_qty,
    COALESCE (ile.kwpurch_qty,0) as non_sellable_kwpurch_qty,
    COALESCE (ile.showroom_m1_floor_qty,0) as non_sellable_showroom_m1_floor_qty,
    COALESCE (ile.abrar_qty,0) as non_sellable_abrar_qty,
    COALESCE (ile.marketing_gift_qty,0) as non_sellable_marketing_gift_qty,
    COALESCE (ile.new_showroom_qty,0) as non_sellable_new_showroom_qty,
    COALESCE(wh_entry.intransit_sys_transfer_trnc_qty,0) as non_sellable_dubai_samp_qty,
    COALESCE (ile.tv_mashour_qty,0) as non_sellable_tv_mashour_qty,
    COALESCE (ile.other_offline_qty,0) as non_sellable_other_offline_qty,
    COALESCE (ile.designer_hamdan_qty,0) as non_sellable_designer_hamdan_qty,
    COALESCE (ile.sample_qty,0) as non_sellable_sample_qty,
    COALESCE (ile.other_qty,0) as non_sellable_other_qty,
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
    phpl.full_pending_open_po_total_qty,
    phpl.partially_pending_open_po_qty,
    phpl.partial_pending_open_po_total_qty,
    phpl.partial_pending_open_po_received_qty,
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
    CAST(item_stock.stock_refreshed_datetime as DATETIME) stock_refreshed_datetime
from [Boutiqaat_Live].[dbo].[Boutiqaat Kuwait\$Item] i
left join 
(
    select 
        [Item No] as sku, 
        sum(case when [Bin Type]='PUTPICK' then [QTY] else 0 end)   as putpick_qty,
        sum(case when [Bin No] IN ('RPASS001','KWI01-IB01-QC01-01A1') then [QTY] else 0 end)  as rtrn_qc_pass,
        sum(case when [Bin No] = 'STAGEMOVEBIN' then [QTY] else 0 end) as stgmv_qty,
        sum(case when [Bin No] = 'CCSTAGE' then [QTY] else 0 end) as ccstage_qty,
        sum(case when [Bin Type] not in ('PUTPICK','RECEIVE') and [Bin No] <> 'RPASS001' then [QTY] else 0 end)   as non_sellable_online_kwi01,
        max([Insert DateTime]) as stock_refreshed_datetime
    from [Boutiqaat_Live].[dbo].[Boutiqaat Kuwait\$Item Stock] 
    group by [Item No]
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
    INNER JOIN [Boutiqaat_Live].[dbo].[Boutiqaat Kuwait\$Stock Details] sd WITH (NOLOCK) 
    ON sd.[Item No] = sd_last_entry.SKU AND sd.[Entry No] = sd_last_entry.MaxEntry 
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
        [Item No] as sku, 
        sum([QTY]) as qty_kwi 
    from [Boutiqaat_Live].[dbo].[Boutiqaat Kuwait\$Item Stock] 
    where [Bin Type] not in ('PUTPICK','RECEIVE') 
    and [Bin No] not in ('RPASS001','STAGEMOVEBIN') 
    and [Location] = 'KWI01'
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
        where [Bin Type] not in ('PUTPICK','RECEIVE') 
        and [Bin No] not in ('RPASS001','STAGEMOVEBIN') 
        and [Location] <> 'KWI01'
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
left join 
(
    select 
        pl.[No_] as sku, 
        SUM(case when ph.[Status] = 1 and COALESCE(pl.[Quantity Received],0)=0 and COALESCE(pl.[Quantity],0)>0 then COALESCE(pl.[Quantity],0) else 0 end) as full_pending_open_po_total_qty, 
        SUM(CASE WHEN ph.[Status] = 1 AND ph.[PO Status] = 2 THEN COALESCE(pl.[Outstanding Quantity],0) ELSE 0 END) partially_pending_open_po_qty,
        SUM(case when ph.[Status] = 1 and COALESCE(pl.[Quantity Received],0)>0 and pl.[Quantity Received]<> pl.[Quantity] then COALESCE(pl.[Outstanding Quantity],0) else 0 end) as partial_pending_open_po_total_qty,
        SUM(case when ph.[Status] = 1 and COALESCE(pl.[Quantity Received],0)>0 and pl.[Quantity Received]<> pl.[Quantity] then COALESCE(pl.[Quantity Received],0) else 0 end) as partial_pending_open_po_received_qty
    from [Boutiqaat_Live].[dbo].[Boutiqaat Kuwait\$Purchase Line] pl
    inner join [Boutiqaat_Live].[dbo].[Boutiqaat Kuwait\$Purchase Header] ph on ph.[No_] = pl.[Document No_]
    and pl.[No_] <> ''
    where ph.[Status] in (1,3) and ph.[Expiry Date] > getdate()
    group by pl.[No_]
) phpl on phpl.sku = i.[No_]
left join 
(
    select 
        [Item No_] as sku,
        sum(case when [Location Code]='DXBSAMPLE' and [Bin Type Code] in ('QC','RECEIVE') then [Quantity] else 0 end) as intransit_sys_transfer_trnc_qty,
        sum(case when [Location Code]='BOR' and [Bin Type Code] in ('QC','RECEIVE') then [Quantity] else 0 end) as old_return_solv_qty,
        sum(case when [Location Code]='KWSAMPLE' and [Bin Type Code] in ('QC','RECEIVE') then [Quantity] else 0 end) as sample_qty
    from [Boutiqaat_Live].[dbo].[Boutiqaat Kuwait\$Warehouse Entry] group by [Item No_]
) wh_entry on wh_entry.sku = i.[No_]
) as sub
WHERE \$CONDITIONS
