SELECT 
    sku, 
    max(vendor_count) as vendor_count, 
    max(max_posting_date) as max_posting_date,
    max(last_item_cost_kwd) as last_item_cost_kwd,
    max(last_item_cost) as last_item_cost, 
    max(last_item_cost_currency) as last_item_cost_currency 
FROM 
(
    SELECT 
        prl.[No_] as sku,  
        DENSE_RANK() over (PARTITION BY prl.[No_] ORDER BY prl.[Buy-from Vendor No_]) as vendor_count,  
        max(prl.[Posting Date]) OVER (PARTITION BY prl.[No_]) as max_posting_date, 
        prl.[Posting Date] as posting_date, 
        CASE WHEN prh.[Currency Code]='' THEN prl.[Unit Cost] ELSE prl.[Unit Cost]/prh.[Currency Factor] END as last_item_cost_kwd,
        prl.[Unit Cost] as last_item_cost,
        prh.[Currency Code] as last_item_cost_currency 
    FROM [Boutiqaat_Live].[dbo].[Boutiqaat Kuwait\$Purch_ Rcpt_ Line]  prl 
    LEFT JOIN [Boutiqaat_Live].[dbo].[Boutiqaat Kuwait\$Purch_ Rcpt_ Header] prh 
    on prl.[Document No_]= prh.No_ 
    WHERE prl.[Quantity]>0 and prl.[Inventory Type] = 0
) purchase where posting_date = max_posting_date AND \$CONDITIONS
group by sku