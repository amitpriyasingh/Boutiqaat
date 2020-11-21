BEGIN;

DROP TABLE IF EXISTS sandbox.attr_actions_201920 ;

SELECT * INTO sandbox.attr_actions_201920 
FROM
(select DATE(activity_at) activity_date, network_partners.* from adjust.network_partners
left join sandbox.network_partner_primary_activity as nppa on network_partners.network_name=nppa.network
where (nppa.network IS NULL OR (nppa.primary_activity='Install' AND network_partners.activity 
IN ('install','install_update','reattribution_update','reinstall','reattribution_reinstall'))) OR((nppa.network IS NOT NULL AND nppa.primary_activity!='Install')
AND network_partners.activity IN ('install','install_update','click','reattribution_update','reinstall','reattribution','reattribution_reinstall')));

DROP TABLE IF EXISTS sandbox.transactions_201920 ;

SELECT * INTO sandbox.transactions_201920
FROM
(select  * from 
(select a.*, ROW_NUMBER() over (partition by tx_number order by order_at desc) as ranking
from adjust.transactions a where tx_number is not null 
union all
select a.*, ROW_NUMBER() over (partition by device_id, order_date, adj_tracker, adj_network_name, bi_revenue order by order_at desc) as ranking 
from adjust.transactions a where tx_number is null
) b where ranking = 1);

DROP TABLE IF EXISTS sandbox.tx_attr_7days_201920;

SELECT * INTO sandbox.tx_attr_7days_201920
FROM
(select a.* from (
select transactions.*,
attr_actions.activity_date as bi_attr_activity_date, 
COALESCE(attr_actions.tracker_name,'Organic') as bi_tracker_name,
attr_actions.last_tracker_name as bi_last_tracker_name,
COALESCE(attr_actions.network_name,'Organic') as bi_network_name,
attr_actions.campaign_name as bi_campaign_name,
attr_actions.adgroup_name as bi_adgroup_name,
attr_actions.creative_name as bi_creative_name,
attr_actions.activity as bi_attr_activity,
attr_actions.activity_at as bi_attr_activity_at,
attr_actions.match_type as bi_match_type,
attr_actions.ingestion_date as bi_ingestion_date,
attr_actions.country as bi_country,
row_number() over (partition by transactions.tx_unique_id, coalesce(transactions.device_id, attr_actions.device_id) order by attr_actions.activity_at desc ) as rno
from sandbox.transactions_201920 as transactions
left join sandbox.attr_actions_201920 as attr_actions on 
(transactions.device_id = attr_actions.device_id and attr_actions.activity_at between (transactions.order_at - interval '7 days') and transactions.order_at)
) a where a.rno = 1); 

DROP TABLE IF EXISTS sandbox.tx_attr_7days_201920_extended;

SELECT * INTO sandbox.tx_attr_7days_201920_extended
FROM
(
   select e.*, 
   e.bi_tracker_name as bi_tracker_name_extended, 
   e.bi_network_name as bi_network_name_extended, 
   e.bi_match_type as bi_match_type_extended, 
   e.bi_attr_activity as bi_attribution_activity_extended, 
   e.bi_attr_activity_at as bi_attribution_activity_at_extended,
   e.bi_campaign_name as bi_campaign_name_extended,
   e.bi_adgroup_name as bi_adgroup_name_extended,
   e.bi_creative_name as bi_creative_name_extended
from sandbox.tx_attr_7days_201920 as e) ;
 
UPDATE sandbox.tx_attr_7days_201920_extended
SET bi_tracker_name_extended=l2_organic_tx_ext.bi_tracker_name_extended,
bi_network_name_extended=l2_organic_tx_ext.bi_network_name_extended,
bi_match_type_extended=l2_organic_tx_ext.bi_match_type_extended,
bi_attribution_activity_extended=l2_organic_tx_ext.bi_attribution_activity_extended,
bi_attribution_activity_at_extended=l2_organic_tx_ext.bi_attribution_activity_at_extended,
bi_campaign_name_extended=l2_organic_tx_ext.bi_campaign_name_extended,
bi_adgroup_name_extended=l2_organic_tx_ext.bi_adgroup_name_extended,
bi_creative_name_extended=l2_organic_tx_ext.bi_creative_name_extended
FROM   (
SELECT transactions.*,
COALESCE(attr_actions.tracker,'j4mcamc')       AS bi_tracker_extended,
COALESCE(attr_actions.tracker_name,'Organic')  AS bi_tracker_name_extended,
COALESCE(attr_actions.network_name,'Organic')  AS bi_network_name_extended,
COALESCE(attr_actions.match_type,'Organic')    AS bi_match_type_extended,
attr_actions.activity                          AS bi_attribution_activity_extended,
attr_actions.activity_at                       AS bi_attribution_activity_at_extended,
attr_actions.campaign_name                     AS bi_campaign_name_extended,
attr_actions.adgroup_name                      AS bi_adgroup_name_extended,
attr_actions.creative_name                     AS bi_creative_name_extended,
row_number() over (partition by transactions.tx_unique_id, transactions.device_id, attr_actions.device_id order by attr_actions.activity_at desc ) as rno
FROM   (
SELECT tx_unique_id, device_id, order_at, bi_attr_activity_at, bi_attr_activity
FROM  sandbox.tx_attr_7days_201920 WHERE  bi_network_name='Organic'
) transactions
JOIN  adjust.attributes_actions AS attr_actions
ON  (transactions.device_id = attr_actions.device_id and attr_actions.activity_at between (transactions.order_at - interval '7 days') and transactions.order_at)
) l2_organic_tx_ext
WHERE  l2_organic_tx_ext.rno = 1 and tx_attr_7days_201920_extended.tx_unique_id = l2_organic_tx_ext.tx_unique_id;

DROP TABLE IF EXISTS sandbox.order_items_with_customer_tags;
CREATE TABLE sandbox.order_items_with_customer_tags AS
SELECT 
   order_item_with_last_order_date.*,
	CASE
      WHEN is_first_order = 1 THEN 'new'
      WHEN datediff(day,last_order_date,order_date) <= 30 THEN 'active'
      WHEN datediff(day,last_order_date,order_date) <= 90 THEN 'inactive'
      WHEN datediff(day,last_order_date,order_date) > 180 THEN 'churn'
      ELSE 'churn'
	END customer_tag 
FROM
(
   SELECT 
      t1.order_number, 
      t1.phone_no, 
      DATE(COALESCE(t1.order_date_utc,t1.order_date)) as order_date, 
      t1.is_first_order, 
      listagg(DISTINCT t1.order_status, ',') as Order_Status,	
      sum (case when t1.order_status not like '%Cancel%' then 1 else 0 end ) as status_count,
      DATE(MAX(COALESCE(t2.order_date_utc,t2.order_date))) last_order_date
   FROM aoi.order_items t1
   LEFT JOIN aoi.order_items t2 
   on t1.phone_no=t2.phone_no and DATE(t2.order_date) between DATE(t1.order_date - INTERVAL '90 DAY') and DATE(t1.order_date - INTERVAL '1 DAY')
   WHERE DATE(t1.order_date) >= '2019-01-01'
   GROUP BY 
      t1.order_number, 
      t1.phone_no, 
      t1.order_date_utc, 
      t1.order_date, 
      t1.is_first_order
) order_item_with_last_order_date;
		
alter table sandbox.order_items_with_customer_tags add column tx_number VARCHAR(255) ENCODE lzo;

update sandbox.order_items_with_customer_tags
set tx_number= reference_number 
from (select reference_number, increment_id order_num, telephone_no from magento.sales_order where DATE(created_at) >= '2019-01-01') 
where telephone_no=phone_no and order_number=order_num;	


DROP TABLE IF EXISTS sandbox.tx_attr_7days_201920_extended_tagged;

SELECT * INTO sandbox.tx_attr_7days_201920_extended_tagged
FROM
(
select t1.*, t2.order_number,t2.phone_no, t2.is_first_order,t2.last_order_date,t2.customer_tag, t2.order_status,
case 
when order_Status is null then 'Not_found_in_OFS'
when order_Status not like '%Cancel%'  then 'Not_Cancelled'
when order_Status like '%Cancel%' and status_count = 0 then 'Full_cancelled'
when order_Status like '%Cancel%' and status_count > 0 then 'Partial_cancelled'
else 'Not_found_in_OFS' end order_status_flag 
from sandbox.tx_attr_7days_201920_extended t1 left join sandbox.order_items_with_customer_tags t2 on t1.tx_number=t2.tx_number); 


DROP TABLE IF EXISTS sandbox.tx_attr_201920_cust_clv;
CREATE TABLE sandbox.tx_attr_201920_cust_clv AS
select
user_id as Device_id,
bi_network_name_extended,
bi_tracker_name_extended,
case when bi_network_name ='Organic' then 'Extended' else 'Basic' END attribution_type_flag,
order_date as transaction_date,tx_number as Transaction_Ref_No,a.order_number,a.phone_no as Phone_Number,CLV_GROSS,CLV_NET
from (select user_id,bi_network_name_extended, bi_tracker_name_extended,bi_network_name, order_date, tx_number,order_number, phone_no
from sandbox.tx_attr_7days_201920_extended_tagged
where bi_network_name_extended <> 'Organic' and customer_tag ='new' group by 1,2,3,4,5,6,7,8
)a
left join
(select  phone_no, sum(net_sale_price_kwd) + sum(shipping_charge_kwd) + sum(cod_charge_kwd) as CLV_GROSS,
sum(case when (order_status not like '%Return%' and order_status not like '%Cancel%') then (net_sale_price_kwd + shipping_charge_kwd + cod_charge_kwd) else 0 end) as CLV_NET
from aoi.order_items where order_category='NORMAL' group by phone_no ) b on a.phone_no = b.phone_no ;

DROP TABLE IF EXISTS sandbox.tx_attr_201920_monthly_clv;
CREATE TABLE sandbox.tx_attr_201920_monthly_clv AS
SELECT
tracking_month, tracking_year, 
adj_network_name as Channel,
count(tx_number) as Transactions_Adj, sum(bi_revenue_usd) Revenue_Adj,
count(case when customer_tag ='new' then tx_unique_id else NULL end)/NULLIF(1- sum(case when tx_number is null then 1 else 0 end )/count(tx_unique_id),0) as New_customer_extrapolated,
count(case when customer_tag ='new' then tx_unique_id else NULL end) as new_customer_transaction,
count(case when customer_tag ='active' then tx_unique_id else NULL end) as active_customer_transaction,
count(case when customer_tag ='inactive' then tx_unique_id else NULL end) as inactive_customer_transaction,
count(case when customer_tag ='churn' then tx_unique_id else NULL end) as churn_customer_transaction,
sum(case when tx_number is not null and order_number is null then 1 else 0 end ) as Invalid_transaction,
sum(case when tx_number is null then 1 else 0 end ) as Ref_missing_transaction,
count(case when customer_tag ='new' then tx_unique_id else NULL end) Total_transactions_from_new_customers_till_date,
1- sum(case when tx_number is null then 1 else 0 end ) / NULLIF(count(tx_unique_id),0)  as Mapping_Coverage,
sum(CLV_GROSS) as CLV_GROSS,
sum(CLV_NET) as CLV_NET
from sandbox.tx_attr_7days_201920_extended_tagged a
left join
(select  phone_no, sum(net_sale_price_kwd) + sum(shipping_charge_kwd) + sum(cod_charge_kwd) as CLV_GROSS,
sum(case when (order_status not like '%Return%' and order_status not like '%Cancel%') then (net_sale_price_kwd + shipping_charge_kwd + cod_charge_kwd) else 0 end) as CLV_NET
from aoi.order_items where order_category='NORMAL' group by phone_no ) b on a.phone_no = b.phone_no
group by tracking_month, tracking_year, adj_network_name;
				 
--=========================================
-- Update Columns
				 
-- ALTER TABLE sandbox.tx_attr_7days_201920_extended_tagged DROP COLUMN day_count;

DROP TABLE IF EXISTS sandbox.customer_order_details;
SELECT * INTO sandbox.customer_order_details
FROM
( SELECT 
      order_item_with_tx_ranking_customer_type.order_number,
      order_item_with_tx_ranking_customer_type.app_order_number,
      order_item_with_tx_ranking_customer_type.phone_no,
      order_item_with_tx_ranking_customer_type.order_date,
      order_item_with_tx_ranking_customer_type.is_first_order,
      order_item_with_tx_ranking_customer_type.tx_ranking,
      CASE
          WHEN tx_ranking <>1 and Last_order_date_customer is null THEN order_date 
          WHEN tx_ranking = 1 and Last_order_date_customer is null THEN order_date 
          ELSE Last_order_date_customer 
       END last_odr_date_cus,
    CASE
          WHEN is_first_order = 1 THEN 'new'
          WHEN tx_ranking = 1 THEN 'new'
          WHEN datediff(day,last_odr_date_cus,order_date) <= 90 THEN 'active'
          WHEN datediff(day,last_odr_date_cus,order_date) <= 180 THEN 'inactive'
          ELSE 'churn'
   END customer_type
FROM
(SELECT t1.order_number,t1.app_order_number,t1.phone_no, DATE(COALESCE(t1.order_date_utc,t1.order_date)) as order_date, t1.is_first_order, 
               DATE(MAX(COALESCE(t2.order_date_utc,t2.order_date))) Last_order_date_customer,
               ROW_NUMBER() over (partition by t1.phone_no order by COALESCE(t1.order_date_utc,t1.order_date) ) as tx_ranking
          FROM aoi.order_items t1
          LEFT JOIN aoi.order_items t2 
             on t1.phone_no=t2.phone_no and DATE(t2.order_date) between DATE(t1.order_date - INTERVAL '90 DAY') and DATE(t1.order_date - INTERVAL '1 DAY')
      WHERE DATE(t1.order_date) >= '2019-01-01'
      GROUP BY t1.order_number,t1.app_order_number,t1.phone_no, t1.order_date_utc, t1.order_date, t1.is_first_order
) order_item_with_tx_ranking_customer_type);

alter table sandbox.tx_attr_7days_201920_extended_tagged add column order_count integer ENCODE lzo;
alter table sandbox.tx_attr_7days_201920_extended_tagged add column customer_type VARCHAR(20) ENCODE lzo;

UPDATE sandbox.tx_attr_7days_201920_extended_tagged
SET order_count = ci.tx_ranking,
    customer_type = ci.customer_type
FROM sandbox.tx_attr_7days_201920_extended_tagged ex JOIN sandbox.customer_order_details ci
ON ci.app_order_number = ex.tx_number;
				 
COMMIT;
