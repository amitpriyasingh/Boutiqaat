BEGIN;
DROP TABLE IF EXISTS analytics.mk_sales_order_items;

SELECT * INTO analytics.mk_sales_order_items
FROM
(
    select
        od.*,
        od.category1 as category_1,
        convert_timezone('AZT', od.order_at_utc) as order_at_dxb,
        NVL2(od.celebrity_id, 1, 0) as is_boutique,
        CASE
            WHEN od.category1 in ('MEN','WOMEN') THEN 'FASHION'
            WHEN od.category1 in ('WOMENSUPPANDFITNESS','MENWOMEN','MENSUPPANDFITNESS') THEN 'FITNESS'
            END as category,
        CASE WHEN od.order_category='CELEBRITY' or order_status='Cancel' or payment_gateway='Boutiqaat Store credit' THEN 0 ELSE (od.net_sale_price_kwd - nsm.last_item_cost_kwd) END as margin,
        nsm.last_item_cost_kwd as unit_cost
    from aoi.order_details od
    left join nav.nav_sku_cost nsm
    on od.sku = nsm.sku
    where od.category1 in ('MEN', 'WOMEN','WOMENSUPPANDFITNESS','MENWOMEN','MENSUPPANDFITNESS')
    AND od.brand not in ('Ducati')
);

grant select on table analytics.mk_sales_order_items to supersetagent;

COMMIT;