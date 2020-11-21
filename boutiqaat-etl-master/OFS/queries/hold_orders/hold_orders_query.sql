SELECT 
    Id,
    WebOrderNo,
    ItemId,
    Reason,
    Notes,
    User,
    InsertedOn,
    Deleted,
    DeletedOn,
    OperationType,
    ReadyForArchive,
    InsertedBy,
    UpdatedOn,
    UpdatedBy
FROM OFS.HoldOrders
WHERE \$CONDITIONS