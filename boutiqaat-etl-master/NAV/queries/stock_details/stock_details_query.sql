SELECT
    [timestamp] as ts, 
    [Entry No] as entry_no, 
    [Item No] as item_no, 
    Location as location, 
    [Qty in Stock] as qty_in_stock, 
    [Stock Type] as stock_type, 
    [Stock Sync ERP] as stock_sync_erp, 
    [Stock Sync ERP Error] as stock_sync_erp_error, 
    [Stock Sync ERP At] as stock_sync_erp_at, 
    [Stock Sync Web] as stock_sync_web, 
    [Stock Sync Web AT] as stock_sync_web_at, 
    [Stock Sync Web Error] as stock_sync_web_error, 
    [Insert At] as insert_at, 
    [Stock Sync NewStack] as stock_sync_newstack, 
    [Stock Sync NewStack At] as stock_sync_newstack_at, 
    [Stock Sync NewStack Msg] as stock_sync_newstack_msg, 
    [Stock Sync NewStack Msg 1] as stock_sync_newstack_msg_1, 
    [Retry Count Web] as retry_count_web, 
    [Retry Count NewStack] as retry_count_newstack, 
    [Retry Count ERP] as retry_count_erp, 
    [Warehouse Entry] as warehouse_entry, 
    [Warehouse JN] as warehouse_jn, 
    [Warehouse Activity Line] as warehouse_activity_line, 
    [Warehouse Activity Line 2] as warehouse_activity_line_2, 
    [Movement Journal Line] as movement_journal_line,
    [Reserved Quantity] as reserved_quantity
FROM Boutiqaat_Live.dbo.[Boutiqaat Kuwait\$Stock Details]
WHERE \$CONDITIONS
