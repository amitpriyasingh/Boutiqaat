SELECT
    [timestamp] as ts,
    [Entry No_] as entry_no,
    [Item No_] as item_no,
    [Posting Date] as posting_date,
    [Entry Type] as entry_type,
    [Source No_] as source_no,
    [Document No_] as document_no,
    REPLACE(REPLACE(REPLACE(REPLACE(Description, CHAR(13), ''), CHAR(10), ''), CHAR(9), ''),'\"', '') as description,
    [Location Code] as location_code,
    Quantity as quantity,
    [Remaining Quantity] as remaining_quantity,
    [Invoiced Quantity] as invoiced_quantity,
    [Applies-to Entry] as applies_to_entry,
    [Open] as is_open,
    [Global Dimension 1 Code] as global_dimension_1_code,
    [Global Dimension 2 Code] as global_dimension_2_code,
    Positive as positive,
    [Source Type] as source_type,
    [Drop Shipment] as drop_shipment,
    [Transaction Type] as transaction_type,
    [Transport Method] as transport_method,
    [Country_Region Code] as country_region_code,
    [Entry_Exit Point] as entry_exit_point,
    [Document Date] as document_date,
    [External Document No_] as external_document_no,
    Area as area,
    [Transaction Specification] as transaction_specification,
    [No_ Series] as no_series,
    [Document Type] as document_type,
    [Document Line No_] as document_line_no,
    [Order Type] as order_type,
    [Order No_] as order_no,
    [Order Line No_] as order_line_no,
    [Dimension Set ID] as dimension_set_id,
    [Assemble to Order] as assemble_to_order,
    [Job No_] as job_no,
    [Job Task No_] as job_task_no,
    [Job Purchase] as job_purchase,
    [Variant Code] as variant_code,
    [Qty_ per Unit of Measure] as qty_per_unit_of_measure,
    [Unit of Measure Code] as unit_of_measure_code,
    [Derived from Blanket Order] as derived_from_blanket_order,
    [Cross-Reference No_] as cross_reference_no,
    [Originally Ordered No_] as originally_ordered_no,
    [Originally Ordered Var_ Code] as originally_ordered_var_code,
    [Out-of-Stock Substitution] as out_of_stock_substitution,
    [Item Category Code] as item_category_code,
    Nonstock as nonstock,
    [Purchasing Code] as purchasing_code,
    [Product Group Code] as product_group_code,
    [Completely Invoiced] as completely_invoiced,
    [Last Invoice Date] as last_invoice_date,
    [Applied Entry to Adjust] as applied_entry_to_adjust,
    Correction as correction,
    [Shipped Qty_ Not Returned] as shipped_qty_not_returned,
    [Prod_ Order Comp_ Line No_] as prod_order_comp_line_no,
    [Serial No_] as serial_no,
    [Lot No_] as lot_no,
    [Warranty Date] as warranty_date,
    [Expiration Date] as expiration_date,
    [Item Tracking] as item_tracking,
    [Return Reason Code] as return_reason_code,
    [Web Order ID] as web_order_id,
    FOC as foc,
    [FOC Reason] as foc_reason,
    [Purchase Order Type] as purchase_order_type,
    Tester as tester,
    [Vendor No_] as vendor_no,
    [Posted On] as posted_on,
    [Inventory Type] as inventory_type,
    [Sales Entry Type] as sales_entry_type
FROM Boutiqaat_Live.dbo.[Boutiqaat Kuwait\$Item Ledger Entry]
WHERE \$CONDITIONS