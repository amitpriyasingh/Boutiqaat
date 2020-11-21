SELECT
    [timestamp] as ts,
    [Item No_] as item_no,
    [Vendor No_] as vendor_no,
    [Start Date] as start_date,
    Brand as brand,
    [Item Category Code 1] as item_category_code_1,
    [Item Category Code 2] as item_category_code_2,
    [Item Category Code 3] as item_category_code_3,
    [Item Category Code 4] as item_category_code_4,
    [Entry No] as entry_no,
    [Retail Price] as retail_price,
    Discount as discount,
    Cost as cost,
    [Cost Price Calculation] as cost_price_calculation,
    [Vendor Discount Type] as vendor_discount_type,
    Blocked as blocked,
    [Vendor Disc_ Line No_] as vendor_disc_line_no
FROM Boutiqaat_Live.dbo.[Boutiqaat Kuwait\$Item Vendor Discount]
WHERE \$CONDITIONS