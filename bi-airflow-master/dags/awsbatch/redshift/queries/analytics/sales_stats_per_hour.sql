BEGIN;
DROP TABLE IF EXISTS analytics.sales_stats_per_hour;
select * into analytics.sales_stats_per_hour
FROM
(select 
    DATE(order_at_dxb) as order_date, 
    DATE_PART(hours, order_at_dxb) day_hour,
    category,
    sum(quantity) sum_qty,
    sum(net_sale_price_kwd) sum_net_sale_price_kwd
from analytics.mk_sales_order_items 
where order_category<>'CELEBRITY' and order_status<>'Cancel'
group by 1,2,3);
COMMIT;