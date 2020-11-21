SELECT 
    Id as id,
    WebOrderNo as web_order_no,
    ItemId as item_id,
    ItemNo as item_no,
    StatusId as status_id,
    SKU as sku,
    ReadyForArchive as ready_for_archive,
    InsertedBy as inserted_by,
    InsertedOn as inserted_on,
    UpdatedBy as updated_by,
    UpdatedOn as updated_on
FROM OFS.OrderStatus
WHERE \$CONDITIONS