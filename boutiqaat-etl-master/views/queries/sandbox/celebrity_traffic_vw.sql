CREATE OR REPLACE VIEW sandbox.celebrity_traffic_vw AS
SELECT
	celebrity_sale_traffic_rank.celebrity_id,
	celebrity_sale_traffic_rank.sorting_rank
FROM
	sandbox.celebrity_sale_traffic_rank
WITH NO SCHEMA BINDING;