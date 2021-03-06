SELECT
    [timestamp] as ts,
    [Document Type] as document_type,
    [Document No_] as document_no,
    [Line No_] as line_no,
    [Buy-from Vendor No_] as buy_from_vendor_no,
    [Type] as type,
    No_ as no,
    [Location Code] as location_code,
    [Posting Group] as posting_group,
    [Expected Receipt Date] as expected_receipt_date,
    REPLACE(REPLACE(REPLACE(REPLACE(Description, CHAR(13), ''), CHAR(10), ''), CHAR(9), ''),'\"', '') as description,
    REPLACE(REPLACE(REPLACE(REPLACE([Description 2], CHAR(13), ''), CHAR(10), ''), CHAR(9), ''),'\"', '') as description_2,
    [Unit of Measure] as unit_of_measure,
    Quantity as quantity,
    [Outstanding Quantity] as outstanding_quantity,
    [Qty_ to Invoice] as qty_to_invoice,
    [Qty_ to Receive] as qty_to_receive,
    [Direct Unit Cost] as direct_unit_cost,
    [Unit Cost (LCY)] as unit_cost_lcy,
    [VAT _] as vat,
    [Line Discount _] as line_discount,
    [Line Discount Amount] as line_discount_amount,
    Amount as amount,
    [Amount Including VAT] as amount_including_vat,
    [Unit Price (LCY)] as unit_price_lcy,
    [Allow Invoice Disc_] as allow_invoice_disc,
    [Gross Weight] as gross_weight,
    [Net Weight] as net_weight,
    [Units per Parcel] as units_per_parcel,
    [Unit Volume] as unit_volume,
    [Appl_-to Item Entry] as appl_to_item_entry,
    [Shortcut Dimension 1 Code] as shortcut_dimension_1_code,
    [Shortcut Dimension 2 Code] as shortcut_dimension_2_code,
    [Job No_] as job_no,
    [Indirect Cost _] as indirect_cost,
    [Recalculate Invoice Disc_] as recalculate_invoice_disc,
    [Outstanding Amount] as outstanding_amount,
    [Qty_ Rcd_ Not Invoiced] as qty_rcd_not_invoiced,
    [Amt_ Rcd_ Not Invoiced] as amt_rcd_not_invoiced,
    [Quantity Received] as quantity_received,
    [Quantity Invoiced] as quantity_invoiced,
    [Receipt No_] as receipt_no,
    [Receipt Line No_] as receipt_line_no,
    [Profit _] as profit,
    [Pay-to Vendor No_] as pay_to_vendor_no,
    [Inv_ Discount Amount] as inv_discount_amount,
    [Vendor Item No_] as vendor_item_no,
    [Sales Order No_] as sales_order_no,
    [Sales Order Line No_] as sales_order_line_no,
    [Drop Shipment] as drop_shipment,
    [Gen_ Bus_ Posting Group] as gen_bus_posting_group,
    [Gen_ Prod_ Posting Group] as gen_prod_posting_group,
    [VAT Calculation Type] as vat_calculation_type,
    [Transaction Type] as transaction_type,
    [Transport Method] as transport_method,
    [Attached to Line No_] as attached_to_line_no,
    [Entry Point] as entry_point,
    Area as area,
    [Transaction Specification] as transaction_specification,
    [Tax Area Code] as tax_area_code,
    [Tax Liable] as tax_liable,
    [Tax Group Code] as tax_group_code,
    [Use Tax] as use_tax,
    [VAT Bus_ Posting Group] as vat_bus_posting_group,
    [VAT Prod_ Posting Group] as vat_prod_posting_group,
    [Currency Code] as currency_code,
    [Outstanding Amount (LCY)] as outstanding_amount_lcy,
    [Amt_ Rcd_ Not Invoiced (LCY)] as amt_rcd_not_invoiced_lcy,
    [Blanket Order No_] as blanket_order_no,
    [Blanket Order Line No_] as blanket_order_line_no,
    [VAT Base Amount] as vat_base_amount,
    [Unit Cost] as unit_cost,
    [System-Created Entry] as system_created_entry,
    [Line Amount] as line_amount,
    [VAT Difference] as vat_difference,
    [Inv_ Disc_ Amount to Invoice] as inv_disc_amount_to_invoice,
    [VAT Identifier] as vat_identifier,
    [IC Partner Ref_ Type] as ic_partner_ref_type,
    [IC Partner Reference] as ic_partner_reference,
    [Prepayment _] as prepayment,
    [Prepmt_ Line Amount] as prepmt_line_amount,
    [Prepmt_ Amt_ Inv_] as prepmt_amt_inv,
    [Prepmt_ Amt_ Incl_ VAT] as prepmt_amt_incl_vat,
    [Prepayment Amount] as prepayment_amount,
    [Prepmt_ VAT Base Amt_] as prepmt_vat_base_amt,
    [Prepayment VAT _] as prepayment_vat,
    [Prepmt_ VAT Calc_ Type] as prepmt_vat_calc_type,
    [Prepayment VAT Identifier] as prepayment_vat_identifier,
    [Prepayment Tax Area Code] as prepayment_tax_area_code,
    [Prepayment Tax Liable] as prepayment_tax_liable,
    [Prepayment Tax Group Code] as prepayment_tax_group_code,
    [Prepmt Amt to Deduct] as prepmt_amt_to_deduct,
    [Prepmt Amt Deducted] as prepmt_amt_deducted,
    [Prepayment Line] as prepayment_line,
    [Prepmt_ Amount Inv_ Incl_ VAT] as prepmt_amount_inv_incl_vat,
    [Prepmt_ Amount Inv_ (LCY)] as prepmt_amount_inv_lcy,
    [IC Partner Code] as ic_partner_code,
    [Prepmt_ VAT Amount Inv_ (LCY)] as prepmt_vat_amount_inv_lcy,
    [Prepayment VAT Difference] as prepayment_vat_difference,
    [Prepmt VAT Diff_ to Deduct] as prepmt_vat_diff_to_deduct,
    [Prepmt VAT Diff_ Deducted] as prepmt_vat_diff_deducted,
    [Outstanding Amt_ Ex_ VAT (LCY)] as outstanding_amt_ex_vat_lcy,
    [A_ Rcd_ Not Inv_ Ex_ VAT (LCY)] as a_rcd_not_inv_ex_vat_lcy,
    [Dimension Set ID] as dimension_set_id,
    [Job Task No_] as job_task_no,
    [Job Line Type] as job_line_type,
    [Job Unit Price] as job_unit_price,
    [Job Total Price] as job_total_price,
    [Job Line Amount] as job_line_amount,
    [Job Line Discount Amount] as job_line_discount_amount,
    [Job Line Discount _] as job_line_discount,
    [Job Unit Price (LCY)] as job_unit_price_lcy,
    [Job Total Price (LCY)] as job_total_price_lcy,
    [Job Line Amount (LCY)] as job_line_amount_lcy,
    [Job Line Disc_ Amount (LCY)] as job_line_disc_amount_lcy,
    [Job Currency Factor] as job_currency_factor,
    [Job Currency Code] as job_currency_code,
    [Job Planning Line No_] as job_planning_line_no,
    [Job Remaining Qty_] as job_remaining_qty,
    [Job Remaining Qty_ (Base)] as job_remaining_qty_base,
    [Deferral Code] as deferral_code,
    [Returns Deferral Start Date] as returns_deferral_start_date,
    [Prod_ Order No_] as prod_order_no,
    [Variant Code] as variant_code,
    [Bin Code] as bin_code,
    [Qty_ per Unit of Measure] as qty_per_unit_of_measure,
    [Unit of Measure Code] as unit_of_measure_code,
    [Quantity (Base)] as quantity_base,
    [Outstanding Qty_ (Base)] as outstanding_qty_base,
    [Qty_ to Invoice (Base)] as qty_to_invoice_base,
    [Qty_ to Receive (Base)] as qty_to_receive_base,
    [Qty_ Rcd_ Not Invoiced (Base)] as qty_rcd_not_invoiced_base,
    [Qty_ Received (Base)] as qty_received_base,
    [Qty_ Invoiced (Base)] as qty_invoiced_base,
    [FA Posting Date] as fa_posting_date,
    [FA Posting Type] as fa_posting_type,
    [Depreciation Book Code] as depreciation_book_code,
    [Salvage Value] as salvage_value,
    [Depr_ until FA Posting Date] as depr_until_fa_posting_date,
    [Depr_ Acquisition Cost] as depr_acquisition_cost,
    [Maintenance Code] as maintenance_code,
    [Insurance No_] as insurance_no,
    [Budgeted FA No_] as budgeted_fa_no,
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
    [Special Order] as special_order,
    [Special Order Sales No_] as special_order_sales_no,
    [Special Order Sales Line No_] as special_order_sales_line_no,
    [Completely Received] as completely_received,
    [Requested Receipt Date] as requested_receipt_date,
    [Promised Receipt Date] as promised_receipt_date,
    [Lead Time Calculation] as lead_time_calculation,
    [Inbound Whse_ Handling Time] as inbound_whse_handling_time,
    [Planned Receipt Date] as planned_receipt_date,
    [Order Date] as order_date,
    [Allow Item Charge Assignment] as allow_item_charge_assignment,
    [Return Qty_ to Ship] as return_qty_to_ship,
    [Return Qty_ to Ship (Base)] as return_qty_to_ship_base,
    [Return Qty_ Shipped Not Invd_] as return_qty_shipped_not_invd,
    [Ret_ Qty_ Shpd Not Invd_(Base)] as ret_qty_shpd_not_invd_base,
    [Return Shpd_ Not Invd_] as return_shpd_not_invd,
    [Return Shpd_ Not Invd_ (LCY)] as return_shpd_not_invd_lcy,
    [Return Qty_ Shipped] as return_qty_shipped,
    [Return Qty_ Shipped (Base)] as return_qty_shipped_base,
    [Return Shipment No_] as return_shipment_no,
    [Return Shipment Line No_] as return_shipment_line_no,
    [Return Reason Code] as return_reason_code,
    FOC as foc,
    [Amount Inc_ Tax (LCY)] as amount_inc_tax_lcy,
    [Ret_ Doc_ No_] as ret_doc_no,
    [Ret_ Doc_ Line No_] as ret_doc_line_no,
    [Contract No_] as contract_no,
    Margin as margin,
    [Add_ Margin] as add_margin,
    [Add_ FOC Quantity] as add_foc_quantity,
    [Purchase Cost] as purchase_cost,
    Tester as tester,
    [EAN Code] as ean_code,
    Narration as narration,
    [Inventory Type] as inventory_type,
    [Item Category 3] as item_category_3,
    [Item Category 4] as item_category_4,
    Brand as brand,
    [Original Purchase Cost] as original_purchase_cost,
    [Cost Updated By] as cost_updated_by,
    FOCPO as focpo,
    [System Calculated FOC] as system_calculated_foc,
    [Routing No_] as routing_no,
    [Operation No_] as operation_no,
    [Work Center No_] as work_center_no,
    Finished as finished,
    [Prod_ Order Line No_] as prod_order_line_no,
    [Overhead Rate] as overhead_rate,
    [MPS Order] as mps_order,
    [Planning Flexibility] as planning_flexibility,
    [Safety Lead Time] as safety_lead_time,
    [Routing Reference No_] as routing_reference_no
FROM Boutiqaat_Live.dbo.[Boutiqaat Kuwait\$Purchase Line]
WHERE \$CONDITIONS