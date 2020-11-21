CREATE OR REPLACE VIEW analytics.events_report_view AS 
SELECT
	emt.event_id,
	emt.events_header_id,
	emt.user_name,
	emt.celebrity_name,
	emt.celebrity_id,
	emt.generic,
	emt.event_portal,
	emt.event_type,
	emt.event_class,
	emt.bq_post,
	emt.total_post,
	emt.event_date,
	emt.event_time,
	emt.created_at,
	emt.updated_at,
	emt.event_hours,
	emt.event_minutes,
	emt.remark,
	emt.labelid,
	emt.skuid,
	emt.product_entity_id,
	COALESCE(sm.sku_name, pc.sku_name) AS sku_name,
	COALESCE(sm.brand, pc.brand) AS brand,
	COALESCE(sm.category1, pc.category1) AS category1,
	COALESCE(sm.category2, pc.category2) AS category2,
	COALESCE(sm.category3, pc.category3) AS category3,
	COALESCE(sm.category4, pc.category4) AS category4,
	am.account_manager,
	am.am_email,
	am.rm_name,
	am.rm_email,
	CASE WHEN (COALESCE(ptype.purchase_type, pc.purchase_type) = 0) THEN
		'Blank'::text
	WHEN (COALESCE(ptype.purchase_type, pc.purchase_type) = 1) THEN
		'Consignment'::text
	WHEN (COALESCE(ptype.purchase_type, pc.purchase_type) = 2) THEN
		'Wholesale'::text
	WHEN (COALESCE(ptype.purchase_type, pc.purchase_type) = 3) THEN
		'Private Label'::text
	WHEN (COALESCE(ptype.purchase_type, pc.purchase_type) = 4) THEN
		'Driver'::text
	WHEN (COALESCE(ptype.purchase_type, pc.purchase_type) = 5) THEN
		'Celebrity'::text
	WHEN (COALESCE(ptype.purchase_type, pc.purchase_type) = 6) THEN
		'Agent'::text
	WHEN (COALESCE(ptype.purchase_type, pc.purchase_type) = 7) THEN
		'Subcon'::text
	WHEN (COALESCE(ptype.purchase_type, pc.purchase_type) = 8) THEN
		'Service'::text
	WHEN (COALESCE(ptype.purchase_type, pc.purchase_type) = 9) THEN
		'Fixed Asset'::text
	WHEN (COALESCE(ptype.purchase_type, pc.purchase_type) = 10) THEN
		'Others'::text
	WHEN (COALESCE(ptype.purchase_type, pc.purchase_type) = 11) THEN
		'IT Services'::text
	WHEN (COALESCE(ptype.purchase_type, pc.purchase_type) = 12) THEN
		'Marketing'::text
	WHEN (COALESCE(ptype.purchase_type, pc.purchase_type) = 13) THEN
		'wholesale At Cost'::text
	WHEN (COALESCE(ptype.purchase_type, pc.purchase_type) = 12) THEN
		' Raw-Material'::text
	ELSE
		NULL::text
	END AS purchase_type,
	((1.0 * (emt.bq_post)::numeric) / (abq.total_sku_endorsed)::numeric) AS alloc_bq_post
FROM bidb.emt_report emt
LEFT JOIN nav.nav_sku_master sm ON sm.sku::text = emt.skuid::text
LEFT JOIN 
(
SELECT
    parent_child.child_sku,
    parent_child.parent_sku,
    parent_child.sku_name,
    parent_child.brand,
    parent_child.category1,
    parent_child.category2,
    parent_child.category3,
    parent_child.category4,
    parent_child.purchase_type,
    parent_child.rownum
FROM 
(
    SELECT
        parent_child_sku_mapping.child_sku,
        parent_child_sku_mapping.parent_sku,
        parent_child_sku_mapping.sku_name,
        parent_child_sku_mapping.brand,
        parent_child_sku_mapping.category1,
        parent_child_sku_mapping.category2,
        parent_child_sku_mapping.category3,
        parent_child_sku_mapping.category4,
        parent_child_sku_mapping.purchase_type,
        pg_catalog.row_number() OVER (PARTITION BY parent_child_sku_mapping.parent_sku) AS rownum
    FROM sandbox.parent_child_sku_mapping) parent_child
	WHERE parent_child.rownum = 1
) pc ON pc.parent_sku::text = emt.skuid::text
LEFT JOIN sandbox.celebrity_am_mapping am ON am.celebrity_id = emt.celebrity_id
AND emt.event_date >= am.am_start_date
AND emt.event_date <= am.am_end_date
LEFT JOIN 
(
	SELECT
		purchase_type.document_no, 
        purchase_type.purchase_type, 
        purchase_type.sku, 
        purchase_type.rownum
	FROM 
    (
        SELECT
            ph. "no" AS document_no,
            ph.purchase_type,
            pl. "no" AS sku,
            pg_catalog.row_number() OVER (PARTITION BY pl. "no") AS rownum
        FROM nav.purchase_header ph
	    JOIN nav.purchase_line pl 
        ON pl.document_no::text = (ph. "no")::text
    ) purchase_type
    WHERE purchase_type.rownum = 1
) ptype 
ON ptype.sku::text = emt.skuid::text
LEFT JOIN 
(
	SELECT
		count(*) AS total_sku_endorsed, 
        emt_report.event_id
	FROM bidb.emt_report
	GROUP BY
		emt_report.event_id
) abq ON emt.event_id::text = abq.event_id::text
WITH NO SCHEMA BINDING;