SELECT
    [timestamp] as ts,
    [Document No_] as document_no,
    [Line No_] as line_no,
    [Sell-to Customer No_] as sell_to_customer_no,
    [Type] as type,
    No_ as no,
    [Location Code] as location_code,
    [Posting Group] as posting_group,
    [Shipment Date] as shipment_date,
    REPLACE(REPLACE(REPLACE(REPLACE(Description, CHAR(13), ''), CHAR(10), ''), CHAR(9), ''),'\"', '') as description,
    REPLACE(REPLACE(REPLACE(REPLACE([Description 2], CHAR(13), ''), CHAR(10), ''), CHAR(9), ''),'\"', '') as description_2,
    [Unit of Measure] as unit_of_measure,
    Quantity as quantity,
    [Unit Price] as unit_price,
    [Unit Cost (LCY)] as unit_cost_lcy,
    [VAT _] as vat,
    [Line Discount _] as line_discount,
    [Line Discount Amount] as line_discount_amount,
    Amount as amount,
    [Amount Including VAT] as amount_including_vat,
    [Allow Invoice Disc_] as allow_invoice_disc,
    [Gross Weight] as gross_weight,
    [Net Weight] as net_weight,
    [Units per Parcel] as units_per_parcel,
    [Unit Volume] as unit_volume,
    [Appl_-to Item Entry] as appl_to_item_entry,
    [Shortcut Dimension 1 Code] as shortcut_dimension_1_code,
    [Shortcut Dimension 2 Code] as shortcut_dimension_2_code,
    [Customer Price Group] as customer_price_group,
    [Job No_] as job_no,
    [Work Type Code] as work_type_code,
    [Shipment No_] as shipment_no,
    [Shipment Line No_] as shipment_line_no,
    [Bill-to Customer No_] as bill_to_customer_no,
    [Inv_ Discount Amount] as inv_discount_amount,
    [Drop Shipment] as drop_shipment,
    [Gen_ Bus_ Posting Group] as gen_bus_posting_group,
    [Gen_ Prod_ Posting Group] as gen_prod_posting_group,
    [VAT Calculation Type] as vat_calculation_type,
    [Transaction Type] as transaction_type,
    [Transport Method] as transport_method,
    [Attached to Line No_] as attached_to_line_no,
    [Exit Point] as exit_point,
    Area as area,
    [Transaction Specification] as transaction_specification,
    [Tax Category] as tax_category,
    [Tax Area Code] as tax_area_code,
    [Tax Liable] as tax_liable,
    [Tax Group Code] as tax_group_code,
    [VAT Clause Code] as vat_clause_code,
    [VAT Bus_ Posting Group] as vat_bus_posting_group,
    [VAT Prod_ Posting Group] as vat_prod_posting_group,
    [Blanket Order No_] as blanket_order_no,
    [Blanket Order Line No_] as blanket_order_line_no,
    [VAT Base Amount] as vat_base_amount,
    [Unit Cost] as unit_cost,
    [System-Created Entry] as system_created_entry,
    [Line Amount] as line_amount,
    [VAT Difference] as vat_difference,
    [VAT Identifier] as vat_identifier,
    [IC Partner Ref_ Type] as ic_partner_ref_type,
    [IC Partner Reference] as ic_partner_reference,
    [Prepayment Line] as prepayment_line,
    [IC Partner Code] as ic_partner_code,
    [Posting Date] as posting_date,
    [Dimension Set ID] as dimension_set_id,
    [Job Task No_] as job_task_no,
    [Job Contract Entry No_] as job_contract_entry_no,
    [Deferral Code] as deferral_code,
    [Variant Code] as variant_code,
    [Bin Code] as bin_code,
    [Qty_ per Unit of Measure] as qty_per_unit_of_measure,
    [Unit of Measure Code] as unit_of_measure_code,
    [Quantity (Base)] as quantity_base,
    [FA Posting Date] as fa_posting_date,
    [Depreciation Book Code] as depreciation_book_code,
    [Depr_ until FA Posting Date] as depr_until_fa_posting_date,
    [Duplicate in Depreciation Book] as duplicate_in_depreciation_book,
    [Use Duplication List] as use_duplication_list,
    [Responsibility Center] as responsibility_center,
    [Cross-Reference No_] as cross_reference_no,
    [Unit of Measure (Cross Ref_)] as unit_of_measure_cross_ref,
    [Cross-Reference Type] as cross_reference_type,
    [Cross-Reference Type No_] as cross_reference_type_no,
    [Item Category Code] as item_category_code,
    Nonstock as nonstock,
    [Purchasing Code] as purchasing_code,
    [Product Group Code] as product_group_code,
    [Appl_-from Item Entry] as appl_from_item_entry,
    [Return Reason Code] as return_reason_code,
    [Allow Line Disc_] as allow_line_disc,
    [Customer Disc_ Group] as customer_disc_group,
    [Sales Order No_] as sales_order_no,
    [BOX ID] as box_id,
    [Item ID] as item_id,
    [Docket No_] as docket_no,
    [Dispatch Location] as dispatch_location,
    [Lot No_] as lot_no,
    [Docket Date] as docket_date,
    [Campaign ID] as campaign_id
FROM Boutiqaat_Live.dbo.[Boutiqaat Kuwait\$Sales Invoice Line]
WHERE \$CONDITIONS