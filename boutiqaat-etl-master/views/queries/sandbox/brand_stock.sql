CREATE OR REPLACE VIEW sandbox.brand_stock as
SELECT
	dims.brand,
	sum(css.soh) AS soh,
	sum(css.ofs_not_picked_or_cancelled) AS reserved
FROM (aoi.consolidated_sku_stock css
	JOIN sandbox.dim_sku dims ON (((dims.sku)::text = (css.sku)::text)))
GROUP BY
	dims.brand
WITH NO SCHEMA BINDING;