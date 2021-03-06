SELECT
    [timestamp],
    [Entry No_],
    [Journal Batch Name],
    [Line No_],
    [Registering Date],
    [Location Code],
    [Zone Code],
    [Bin Code],
    REPLACE(REPLACE(REPLACE(REPLACE(Description, CHAR(13), ''), CHAR(10), ''), CHAR(9), ''),'\"', '') as description,
    [Item No_],
    Quantity,
    [Qty_ (Base)],
    [Source Type],
    [Source Subtype],
    [Source No_],
    [Source Line No_],
    [Source Subline No_],
    [Source Document],
    [Source Code],
    [Reason Code],
    [No_ Series],
    [Bin Type Code],
    Cubage,
    Weight,
    [Journal Template Name],
    [Whse_ Document No_],
    [Whse_ Document Type],
    [Whse_ Document Line No_],
    [Entry Type],
    [Reference Document],
    [Reference No_],
    [User ID],
    [Variant Code],
    [Qty_ per Unit of Measure],
    [Unit of Measure Code],
    [Serial No_],
    [Lot No_],
    [Warranty Date],
    [Expiration Date],
    [Phys Invt Counting Period Code],
    [Phys Invt Counting Period Type],
    Dedicated,
    [Posted In Queue],
    [Insert Date Time],
    [Batch No_],
    Closed,
    Adjustment
FROM Boutiqaat_Live.dbo.[Boutiqaat Kuwait\$Warehouse Entry]
WHERE \$CONDITIONS