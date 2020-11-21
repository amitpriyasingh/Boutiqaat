SELECT 
    DISTINCT i.[No_]  sku,
    REPLACE(REPLACE(REPLACE(REPLACE(i.[Description], CHAR(13), ''), CHAR(10), ''), CHAR(9), ''),'\"', '') as sku_name,
    i.[EAN Code] bar_code, 
    i.[Brand] as brand_code, 
    i.Color as supplier_color,
    i.Department as department,
    i.[Category Manager] as category_manager,
    iam.[Description] as brand,
    i.[Item Category Code] as category1, 
    i.[Product Group Code] as category2,  
    i.[3rd Category] as category3, 
    i.[4th Category] as category4,
    sale_price.first_selling_price,
    sale_price.last_selling_price,
    itemcost.supplier_cost, 
    itemcost.landed_cost,
    CONVERT(date,sale_price.first_price_entry_time_utc) as first_price_entry_date,
    CONVERT(date,sale_price.last_price_entry_time_utc) as last_price_entry_date, 
    purch_rctp_line.last_item_cost,
    shippingcost.shipping_cost_per_unit,
    purch_rctp_line.last_item_cost_currency,
    vendor.vendor_no,
    vendor.vendor_name,
    vendor.country_code,
    vendor.vendor_contract_type, 
    vendor.payment_term_code,
    REPLACE(REPLACE(REPLACE(REPLACE(vendor.vendor_item_no, CHAR(13), ''), CHAR(10), ''), CHAR(9), ''),'\"', '') as vendor_item_no,
    CONVERT(date,grn_date.first_grn_date) as first_grn_date,
    CONVERT(date,grn_date.last_grn_date) as last_grn_date,
    grn_days.grn_qty_yesterday,
    grn_days.grn_qty_last_2nd_day,
    grn_days.grn_qty_last_3rd_day,
    grn_days.grn_qty_last_4th_day,
    grn_days.grn_qty_last_5th_day,
    grn_days.grn_qty_last_6th_day,
    grn_days.grn_qty_last_7th_day,
    grn.grn_qty_2020,
    grn.grn_value_2020,
    grn.sku_avg_cost_2020,
    grn.grn_qty_2019,
    grn.grn_value_2019,
    grn.sku_avg_cost_2019,
    grn.grn_qty_2018,
    grn.grn_value_2018,
    grn.sku_avg_cost_2018,
    grn.grn_qty_2017,
    grn.grn_value_2017,
    grn.sku_avg_cost_2017,
    grn.grn_qty_2016,
    grn.grn_value_2016,
    grn.sku_avg_cost_2016,
    grn.grn_qty_2015,
    grn.grn_value_2015,
    grn.sku_avg_cost_2015,
    putaway.last_putaway_date 
FROM [Boutiqaat_Live].[dbo].[Boutiqaat Kuwait\$Item]  i 
LEFT JOIN dwh.dbo.nav_unisoft_selling_price sale_price on sale_price.sku = i.[No_]
LEFT JOIN [Boutiqaat_Live].[dbo].[Boutiqaat Kuwait\$Item Attribute Master] iam WITH (NOLOCK) on iam.[Code] = i.[Brand]
LEFT JOIN 
(
    SELECT 
    ILE.[Item No_],
    SUM(ILE.Quantity) as total_quantity,
    SUM(case when VLE.[Entry Type]=0 then (VLE.[Cost Amount (Actual)] + VLE.[Cost Amount (Expected)]) else 0 end) as supplier_cost,
    SUM(VLE.[Cost Amount (Actual)] + VLE.[Cost Amount (Expected)]) AS landed_cost 
FROM [Boutiqaat_Live].[dbo].[Boutiqaat Kuwait\$Item Ledger Entry] ILE(NOLOCK)
INNER JOIN [Boutiqaat_Live].[dbo].[Boutiqaat Kuwait\$Value Entry] VLE(NOLOCK) ON ILE.[Entry No_] = VLE.[Item Ledger Entry No_]
Where ILE.Positive = 1  
GROUP BY ILE.[Item No_]
) itemcost on itemcost.[Item No_] = i.[No_]
LEFT JOIN 
(
    SELECT [No_], max(vendor_count) as vendor_count, max(max_posting_date) as max_posting_date,
    max(last_item_cost) as last_item_cost, max(last_item_cost_currency) as last_item_cost_currency from 
    (
        SELECT 
            prl.[No_],  
            DENSE_RANK() over (PARTITION BY prl.[No_] ORDER BY prl.[Buy-from Vendor No_]) as vendor_count,  
            max(prl.[Posting Date]) OVER (PARTITION BY prl.[No_]) as max_posting_date, 
            prl.[Posting Date] as posting_date, 
            prl.[Unit Cost] as last_item_cost, 
            prh.[Currency Code] as last_item_cost_currency 
        FROM [Boutiqaat_Live].[dbo].[Boutiqaat Kuwait\$Purch_ Rcpt_ Line]  prl 
        LEFT JOIN [Boutiqaat_Live].[dbo].[Boutiqaat Kuwait\$Purch_ Rcpt_ Header] prh on prl.[Document No_]= prh.No_ 
        WHERE prl.[Quantity]>0 and prl.[Inventory Type] = 0
    ) purchase where posting_date = max_posting_date group by [No_]
) purch_rctp_line on purch_rctp_line.[No_]=i.[No_]
LEFT JOIN dwh.dbo.nav_unisoft_grn_date as grn_date on grn_date.sku COLLATE SQL_Latin1_General_CP1_CI_AS = i.[No_]
LEFT JOIN dwh.dbo.nav_unisoft_qty_cost_pivot grn on grn.sku = i.[No_]
LEFT JOIN dwh.dbo.day_wise_grn grn_days on grn_days.sku = i.[No_]
LEFT JOIN 
(
    select sku, STRING_AGG(vendor_name,'|') WITHIN GROUP (ORDER BY vendor_name ASC) as vendor_name,
        STRING_AGG(vendor_no,'|') WITHIN GROUP (ORDER BY vendor_name ASC) as vendor_no,
        STRING_AGG(vendor_item_no,'|')  WITHIN GROUP (ORDER BY vendor_name ASC) as vendor_item_no,
        STRING_AGG(country_code,'|')  WITHIN GROUP (ORDER BY vendor_name ASC) as country_code,
        STRING_AGG(vendor_contract_type,'|') WITHIN GROUP (ORDER BY vendor_name ASC) as vendor_contract_type,
        STRING_AGG(payment_term_code,'|') WITHIN GROUP (ORDER BY vendor_name ASC) as payment_term_code from
        (SELECT 
            distinct iv.[Item No_] as sku, 
            iv.[Vendor No_] as vendor_no, 
            iv.[Vendor Item No_] as vendor_item_no,
            v.[Name] as vendor_name, 
            v.[Country_Region Code] as country_code,  
            (case  when v.[Vendor Contract Type] = 0 then 'Consignment' 
                    when v.[Vendor Contract Type] = 1 then 'Wholesale' 
                    when v.[Vendor Contract Type] = 2 then 'Private Label' 
                    when v.[Vendor Contract Type] = 3 then 'Others' 
                    when v.[Vendor Contract Type] = 4 then 'Wholesale' 
                    when v.[Vendor Contract Type] = 5 then 'At Cost' 
                    when v.[Vendor Contract Type] = 6 then 'Raw-Material' 
                    when v.[Vendor Contract Type] = 7 then 'Celebrity' 
                    else NULL
            end ) as vendor_contract_type, 
            v.[Payment Terms Code] as payment_term_code 
        FROM [Boutiqaat_Live].[dbo].[Boutiqaat Kuwait\$Item Vendor] iv 
        JOIN [Boutiqaat_Live].[dbo].[Boutiqaat Kuwait\$Vendor] v 
        on v.[No_] = iv.[Vendor No_]
        ) vendor_agg
        GROUP BY sku
    ) vendor on vendor.sku = i.[No_]
LEFT JOIN 
(
    SELECT
        VLE.[Item No_],
        max(VLE.[Cost per Unit]) as shipping_cost_per_unit  
    from [Boutiqaat_Live].[dbo].[Boutiqaat Kuwait\$Value Entry] VLE(NOLOCK)  
    inner join 
    (
        SELECT 
            [Item No_],
            max([Posting Date]) as max_date 
        from [Boutiqaat_Live].[dbo].[Boutiqaat Kuwait\$Value Entry] 
        group by [Item No_]
    ) AVE on AVE.[Item No_] = VLE.[Item No_] and AVE.max_date = VLE.[Posting Date]
    where VLE.[Item Charge No_]='FREIGHT' group by VLE.[Item No_]
)shippingcost on shippingcost.[Item No_]=i.[No_]  
LEFT JOIN 
(
    SELECT
    sku,max(no_) as no_, max(registering_date) as registering_date, max(last_putaway_date) as last_putaway_date
    from 
    (
        SELECT 
            rh.[No_] as no_,
            rh.[Registering Date] as registering_date,
            rl.[Item No_] as sku,
            max(rh.[Registering Date]) OVER (PARTITION BY rl.[Item No_]) as last_putaway_date 
        from [Boutiqaat_Live].[dbo].[Boutiqaat Kuwait\$Registered Whse_ Activity Hdr_] rh 
        inner join [Boutiqaat_Live].[dbo].[Boutiqaat Kuwait\$Registered Whse_ Activity Line] rl 
        on rl.[No_]=rh.[No_]
    ) ptawy where last_putaway_date = registering_date group by sku
) as putaway on putaway.sku = i.[No_]
WHERE \$CONDITIONS
