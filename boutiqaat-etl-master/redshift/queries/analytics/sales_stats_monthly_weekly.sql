BEGIN;
DROP TABLE IF EXISTS analytics.sales_stats_monthly_weekly;

select * into analytics.sales_stats_monthly_weekly
FROM
(select 
    order_at_dxb,
    DATE_PART(month, order_at_dxb) day_month,
    DATE_PART(week, order_at_dxb) day_week,
    DATE_PART(hours, order_at_dxb) day_hour,
    category,
    category_1,
    brand,
    sum(quantity) sum_qty,
    sum(net_sale_price_kwd) sum_net_sale_price_kwd
from analytics.mk_sales_order_items 
where order_category<>'CELEBRITY' and order_status<>'Cancel'
group by 1,2,3,4,5,6,7);
COMMIT;