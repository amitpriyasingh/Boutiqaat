SELECT
    history_id,
    balance_id,
    updated_at,
    action,
    balance_amount,
    balance_delta,
    REPLACE(REPLACE(REPLACE(REPLACE(TRIM(additional_info), '\r', ''),'\n',''),'\t',''),'\"', '') as additional_info,
    is_customer_notified,
    REPLACE(REPLACE(REPLACE(REPLACE(TRIM(ticket), '\r', ''),'\n',''),'\t',''),'\"', '') as ticket
FROM boutiqaat_v2.magento_customerbalance_history
WHERE \$CONDITIONS