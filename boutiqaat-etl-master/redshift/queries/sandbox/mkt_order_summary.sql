BEGIN;

DROP TABLE IF EXISTS sandbox.mkt_order_summary;

SELECT * INTO sandbox.mkt_order_summary 
FROM
(
select
       order_date,
       adj_network_name as network_name,
       adj_campaign_name as campaign_name,
       adj_adgroup_name as adgroup_name,
       adj_creative_name as creative_name,
       ROUND((SUM(COALESCE(bi_revenue_usd,0))),3)as gross_revenue,
       ROUND((sum(case when lower(order_status) not like '%can%' then COALESCE(bi_revenue_usd,0) else 0 end )),3) as net_revenue,
       SUM(CASE WHEN tx_number is not null THEN 1 ELSE 0 END) as gross_order,
       SUM(CASE WHEN tx_number is not null AND lower(order_status) not like '%can%' then 1 else 0 end ) as net_order
from sandbox.tx_attr_7days_201920_extended_tagged 
where order_date>='2020-03-01' and lower(adj_network_name) not like '%organic%' 
group by order_date,network_name,campaign_name,adgroup_name,creative_name,adj_network_name
order by order_date DESC
);

COMMIT;
