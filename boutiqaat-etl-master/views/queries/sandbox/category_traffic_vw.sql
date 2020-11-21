CREATE OR REPLACE VIEW sandbox.category_traffic_vw AS
SELECT
	category_sale_traffic_rank.category_id,
	category_sale_traffic_rank.sorting_rank
FROM
	sandbox.category_sale_traffic_rank
WITH NO SCHEMA BINDING;