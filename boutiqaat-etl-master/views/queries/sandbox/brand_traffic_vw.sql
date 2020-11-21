CREATE OR REPLACE VIEW sandbox.brand_traffic_vw AS
SELECT
	brand_sale_traffic_rank.brand_id,
	brand_sale_traffic_rank.sorting_rank
FROM
	sandbox.brand_sale_traffic_rank
WITH NO SCHEMA BINDING;