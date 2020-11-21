CREATE OR REPLACE VIEW sandbox.celebrity_am_mapping AS
SELECT
	bcm.celebrity_id,
	bcm.am_email,
	bcm.rm_email,
	bcm.current_am AS account_manager,
	bcm.code AS celebrity_code,
	bcm.am_effective_date AS am_start_date,
	COALESCE(bcm.am_mapping_end_date, '2100-01-01 00:00:00'::timestamp without time zone) AS am_end_date,
	bcm.celebrity_name,
	bcm.rm_name
FROM
	aoi.bi_celebrity_master bcm
WHERE (bcm.is_entry_active = 1)
GROUP BY
	bcm.celebrity_id,
	bcm.am_email,
	bcm.rm_email,
	bcm.current_am,
	bcm.code,
	bcm.am_effective_date,
	bcm.am_mapping_end_date,
	bcm.celebrity_name,
	bcm.rm_name
WITH NO SCHEMA BINDING;