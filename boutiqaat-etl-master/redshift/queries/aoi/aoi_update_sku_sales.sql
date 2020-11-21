DROP TABLE IF EXISTS tmp_sku_sales;
create TEMPORARY TABLE tmp_sku_sales
as
(
    select 
        order_date, 
        sku, 
        sku_name, 
        category_1, 
        category_2, 
        brand, 
        SUM(quantity) sold_qtys,
        GETDATE() as synched_at_utc
    from aoi.sales_order_items 
    where order_category != 'CELEBRITY' 
    AND order_status != 'Cancel' 
    AND order_date >=DATE(DATEADD(day,-30,order_date))
    group by 1,2,3,4,5,6
);

BEGIN;
DELETE FROM aoi.ofs_sku_sales 
USING tmp_sku_sales
WHERE aoi.ofs_sku_sales.order_date=tmp_sku_sales.order_date 
AND aoi.ofs_sku_sales.sku=tmp_sku_sales.sku 
AND aoi.ofs_sku_sales.sku is not null;
INSERT INTO aoi.ofs_sku_sales
SELECT * FROM tmp_sku_sales;
COMMIT;