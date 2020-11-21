BEGIN;
DROP TABLE IF EXISTS tmp_cost_data_v2;

CREATE TEMP TABLE tmp_cost_data_v2(
	date varchar(40),
	network_name_cost VARCHAR(40),
	cost_usd varchar(40)
);


copy tmp_cost_data_v2 from 's3://btq-etl/test/garima/cost_consolidated_data.csv'
iam_role 'arn:aws:iam::652586300051:role/Redshift-athena-S3-readonly'
region 'eu-west-1'
acceptinvchars
IGNOREHEADER 1
CSV;



DROP TABLE  IF EXISTS sandbox.cost_data_v2;
SELECT * INTO sandbox.cost_data_v2
FROM
(
	select 
		DATE(date) as tracking_date, 
		trim(network_name_cost) as network_name, 
		cast(regexp_replace(replace(cost_usd,'-',0), '[^\.[0-9]') as float) cost_usd, 
        GETDATE() as sync_date 
	from tmp_cost_data_v2
);


DROP TABLE IF EXISTS revenue_network_name_group;
CREATE TEMP TABLE revenue_network_name_group(
	network_name varchar(40),
	network_name_final VARCHAR(40)
);

copy revenue_network_name_group from 's3://btq-etl/test/garima/revenue_netwoek_name_group.csv'
iam_role 'arn:aws:iam::652586300051:role/Redshift-athena-S3-readonly'
region 'eu-west-1'
acceptinvchars
IGNOREHEADER 1
CSV;

DROP TABLE IF EXISTS sandbox.revenue_network_grouped;
SELECT * INTO sandbox.revenue_network_grouped
FROM
(
	SELECT 
		trim(network_name) as network_name, 
		trim(network_name_final) as network_name_final 
	from revenue_network_name_group
);
   
DROP TABLE IF EXISTS cost_network_group;
CREATE TEMP TABLE cost_network_group(
	network_name varchar(40),
	network_name_final VARCHAR(40)
);

copy cost_network_group from 's3://btq-etl/test/garima/cost_network_group.csv'
iam_role 'arn:aws:iam::652586300051:role/Redshift-athena-S3-readonly'
region 'eu-west-1'
acceptinvchars
IGNOREHEADER 1
CSV;

DROP TABLE IF EXISTS sandbox.cost_network_group;
SELECT * INTO sandbox.cost_network_group
FROM (select * from cost_network_group);


DROP TABLE IF EXISTS sandbox.mkt_cost_data_grp_v2;
CREATE TABLE sandbox.mkt_cost_data_grp_v2 as
(
	SELECT 
		tracking_date, 
		network_name, 
		SUM(cost_usd) as cost_usd 
	FROM sandbox.cost_data_v2 GROUP BY 1,2
); 


DROP TABLE IF EXISTS sandbox.mkt_cost_customer_funnel_v2;

CREATE TABLE sandbox.mkt_cost_customer_funnel_v2 as
(
  select 
  bi_attr_activity_date, 
  customer_tags, 
  revenue_mapped_network,
  COUNT(distinct CASE when lower(customer_tags)='new' THEN phone_no ELSE NULL END) new_customers,
  SUM(bi_revenue_usd) revenue_USD
  from 
      (select bi_revenue_usd, phone_no, 
       CASE when tx_number is null then 'Ref missing' 
          when tx_number is not null and customer_tag is null then 'invalid'
          ELSE customer_tag 
       END customer_tags,
       Date(bi_attribution_activity_at_extended) AS bi_attr_activity_date,
       bi_network_name_extended AS revenue_mapped_network
      FROM sandbox.tx_attr_7days_201920_extended_tagged
      where tracking_date>='2019-07-23' 
      and lower(bi_network_name_extended) not like '%organic%'
      and (lower(bi_network_name) not like '%organic%' or customer_tag in('new','churn'))
      ) uniq_tx_attr 
GROUP BY 1,2,3
);



DROP  Table IF EXISTS sandbox.roi_automation_outputs_v2;
create table sandbox.roi_automation_outputs_v2 as
select 
	COALESCE(t.bi_attr_activity_date,t3.tracking_date ) as bi_activity_date, 
	to_char(COALESCE(t.bi_attr_activity_date,t3.tracking_date ),'YYYYMM') as bi_month_year,
	t.new_customers,
	t.revenue_usd_new,
	t.revenue_usd_active,
	t.revenue_usd_inactive,
	t.revenue_usd_churn,
	t.revenue_usd_other,
	t.revenue_usd_all,
	COALESCE(t3.network_name,t.network_name_grp) as network_name_grp ,
	t3.cost_usd 
from 
(
 	select 
	 	t1.bi_attr_activity_date, 
		t2.network_name_final as network_name_grp ,
		sum(t1.new_customers) as new_customers,
        sum(case when lower(t1.customer_tags) ='new' then t1.revenue_usd else 0 end) as revenue_usd_new,
        sum(case when lower(t1.customer_tags) ='active' then t1.revenue_usd else 0 end) as revenue_usd_active,
        sum(case when lower(t1.customer_tags) ='inactive' then t1.revenue_usd else 0 end) as revenue_usd_inactive,
        sum(case when lower(t1.customer_tags) ='churn' then t1.revenue_usd else 0 end) as revenue_usd_churn,
        sum(case when lower(t1.customer_tags) ='ref missing' then t1.revenue_usd else 0 end) as revenue_usd_other,
        Sum(t1.revenue_usd) as revenue_usd_all
	FROM  sandbox.mkt_cost_customer_funnel_v2 t1
	LEFT JOIN sandbox.revenue_network_grouped t2  
	ON t2.network_name = t1.revenue_mapped_network 
	where t1.customer_tags <> 'invalid'
	GROUP BY 1,2
) t 
full outer JOIN sandbox.mkt_cost_data_grp_v2 t3  
ON  t.network_name_grp = t3.network_name and t.bi_attr_activity_date = t3.tracking_date ;

COMMIT;
