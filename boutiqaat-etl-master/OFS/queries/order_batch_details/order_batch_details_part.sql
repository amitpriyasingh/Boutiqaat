SELECT
    WebOrderNo,
    ReferenceOrderNo,
    ItemId,
    BatchId,
    OrderType,
    DeliveryType,
    PickLocation,
    VendorID,
    SentforPick,
    TotalItemCount,
    IsSurface,
    IsFOC,
    OrderCategory,
    PackagingLocation,
    ItemNo,
    REPLACE(REPLACE(REPLACE(REPLACE(TRIM(Error), '\r', ''),'\n',''),'\t',''),'\"', '') as error,
    ReadyForArchive,
    InsertedOn,
    InsertedBy,
    UpdatedOn,
    UpdatedBy
FROM OFS.OrderBatchDetails
WHERE DATE(InsertedOn) = DATE('{{DATE}}') AND \$CONDITIONS