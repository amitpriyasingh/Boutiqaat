SELECT
    [timestamp] as ts,
    [Vendor No_] as vendor_no,
    [Item No_] as item_no,
    [Variant Code] as variant_code,
    [Lead Time Calculation] as lead_time_calculation,
    REPLACE(REPLACE(REPLACE(REPLACE([Vendor Item No_], CHAR(13), ''), CHAR(10), ''), CHAR(9), ''),'\"', '') as vendor_item_no,
    [Start Date] as start_date,
    [End Date] as end_date,
    Status as status,
    [Modified By] as modified_by,
    [Purchase Price] as purchase_price,
    [Item Category Code 1] as item_category_code_1,
    [Item Category Code 2] as item_category_code_2,
    [Item Category Code 3] as item_category_code_3,
    [Item Category Code 4] as item_category_code_4,
    Brand as brand,
    [Name (In English)] as name_in_english,
    [Name (In Arabic)] as name_in_arabic
FROM Boutiqaat_Live.dbo.[Boutiqaat Kuwait\$Item Vendor]
WHERE \$CONDITIONS