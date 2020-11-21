CREATE OR REPLACE VIEW analytics.celeb_master_filter AS
SELECT bcm.celebrity_name, bcm.current_am AS account_manager, bcm.am_email, bcm.rm_email FROM aoi.bi_celebrity_master bcm WHERE (((bcm.is_entry_active = 1) AND (bcm.am_effective_date <= ('now'::text)::date)) AND ((bcm.am_mapping_end_date >= ('now'::text)::date) OR (bcm.am_mapping_end_date IS NULL)))
WITH NO SCHEMA BINDING;