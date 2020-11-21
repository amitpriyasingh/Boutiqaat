BEGIN;
DROP TABLE IF EXISTS analytics.nav_item_sales_price_history;

SELECT * INTO analytics.nav_item_sales_price_history
FROM
(select 
	ROW_NUMBER () OVER( PARTITION BY no,gen_bus_posting_group ORDER BY posting_date desc) as rank,
	posting_date,
	no as sku,
	gen_bus_posting_group as web_store,
	unit_price as sales_price,
	amount_including_vat as sales_vat_price,
	unit_cost_lcy as cost_price
from nav.sales_invoice_line);
COMMIT;