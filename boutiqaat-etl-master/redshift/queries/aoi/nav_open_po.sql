
BEGIN;
DROP TABLE IF EXISTS aoi.open_po_log;

SELECT * INTO aoi.open_po_log
FROM 
(SELECT 
    ph.no AS po_number, 
    ph.status AS status_code, 
    (CASE ph.status 
        WHEN 0 THEN 'Open' 
        WHEN 1 THEN 'Released' 
        WHEN 2 THEN 'Pending Approval' 
        WHEN 3 THEN 'Pending Prepayment' 
        WHEN 4 THEN 'Cancelled' 
        WHEN 5 THEN 'Short Close' 
        WHEN 6 THEN 'Pending Cancellation' 
        WHEN 7 THEN 'Closed' 
        ELSE 'Other' END) status_name,
    ph.po_status po_status_code, 
    (CASE 
        WHEN ph.status = 3 
            OR (ph.status = 1 AND ph.po_status = 1) THEN 'Open PO - pending receipt' 
        WHEN ph.status = 1 AND ph.po_status = 2 THEN 'Open PO - partially received' 
        WHEN ph.status = 6 THEN 'Open PO - pending cancellation' 
        ELSE (CASE ph.status 
                WHEN 0 THEN 'Open' 
                WHEN 1 THEN 'Released' 
                WHEN 2 THEN 'Pending Approval' 
                WHEN 3 THEN 'Pending Prepayment' 
                WHEN 4 THEN 'Cancelled' 
                WHEN 5 THEN 'Short Close' 
                WHEN 6 THEN 'Pending Cancellation' 
                WHEN 7 THEN 'Closed' 
                ELSE 'Other' END) 
        END) po_status_name,
    ph.order_date as po_date,
    ph.payment_terms_code as payment_terms_code,
    ph.expiry_date as po_expiry_date, 
    (CASE 
        WHEN ph.status IN (1,3,6) THEN 1 
        ELSE  0 END) open_for_GRN, 
    pl.no AS sku, 
    pl.description AS sku_name, 
    SUM(COALESCE(pl.quantity,0)) AS total_quantity, 
    SUM(COALESCE(pl.outstanding_quantity,0)) AS open_quantity,
    GETDATE() as updated_at_utc
FROM nav.purchase_header ph
LEFT JOIN nav.purchase_line pl
ON ph.no = pl.document_no
GROUP BY pl.document_no, pl.no, pl.description, ph.no,ph.status, ph.order_date, ph.payment_terms_code, ph.expiry_date, ph.po_status
);

DROP TABLE IF EXISTS aoi.open_po;

SELECT * INTO aoi.open_po
FROM(SELECT
    po_number, 
    sku, 
    po_status_code, 
    po_status_name,
    po_date,
    payment_terms_code,
    po_expiry_date,
    open_for_grn,
    sku_name, 
    total_quantity,
    SUM(CASE open_for_GRN WHEN 1 then open_quantity ELSE 0 END) as total_open_quantity, 
    SUM(CASE 
            WHEN po_status_name='Open PO - pending receipt' AND open_for_GRN=1 THEN open_quantity 
            ELSE 0 END) as pending_receipt_quantity, 
    SUM(CASE 
            WHEN po_status_name='Open PO - partially received' AND open_for_GRN=1 THEN open_quantity 
            ELSE 0 END) as partially_received_quantity, 
    SUM(CASE 
            WHEN po_status_name='Open PO - pending cancellation' AND open_for_GRN=1 THEN open_quantity 
            ELSE 0 END) as pending_cancellation_quantity,
    MAX(updated_at_utc) + interval '3 hours' as synched_at 
FROM aoi.open_po_log
GROUP BY 1,2,3,4,5,6,7,8,9,10);

COMMIT;