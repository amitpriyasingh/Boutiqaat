BEGIN;
DROP TABLE IF EXISTS sandbox.instock_disabled_sku;
SELECT * INTO sandbox.instock_disabled_sku
FROM
(
SELECT
	sku_live_status_report.sku,
	sku_live_status_report.description,
	sku_live_status_report.brand,
	sku_live_status_report.category1,
	sku_live_status_report.category2,
	sku_live_status_report.category3,
	sku_live_status_report.category4,
	sku_live_status_report.sync_from_nav_to_magento,
	sku_live_status_report.quantity_available_for_sale,
	sku_live_status_report.warehouse_reserved_qty,
	sku_live_status_report.is_quantity_sync_to_crs,
	sku_live_status_report.stock_entry_no,
	sku_live_status_report.magento_enabled_status,
	sku_live_status_report.magento_sold_out_status,
	sku_live_status_report.synched_at_utc + interval '3 hours' AS report_KW_time
FROM
	analytics.sku_live_status_report
WHERE
	sku_live_status_report.quantity_available_for_sale > 0
	AND(
		sku_live_status_report.magento_enabled_status ::text = 'disabled' ::text
		OR(sku_live_status_report.magento_enabled_status::text = 'enabled'::text
			AND sku_live_status_report.magento_sold_out_status = 1)
		OR sku_live_status_report.sync_from_nav_to_magento = 0)
);
COMMIT;
