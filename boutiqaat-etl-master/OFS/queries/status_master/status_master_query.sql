SELECT 
    ID as id,
    StatusName as status_name,
    ReadyForArchive as ready_for_archive,
    InsertedBy as inserted_by,
    InsertedOn as inserted_on,
    UpdatedBy as updated_by,
    UpdatedOn as updated_on,
    IsCancelable as is_cancelable,
    IsHoldable as is_holdable,
    IsAddressChange as is_address_change,
    ProcessSequence as process_sequence,
    NextProcess as next_process,
    ApplicableForRule as applicable_for_rule,
    IsExchangeable as is_exchangeable,
    IsWaiverOffAble as is_waiver_off_able,
    IsBulkStatusUpdate as is_bulk_status_update,
    IsOrderEditable as is_order_editable,
    SMSRequired as sms_required,
    IsReturnable as is_returnable,
    GWPAdded as gwp_added,
    CRM as crm,
    OFS as ofs,
    IsAssignable as is_assignable
FROM OFS.StatusMaster
WHERE \$CONDITIONS