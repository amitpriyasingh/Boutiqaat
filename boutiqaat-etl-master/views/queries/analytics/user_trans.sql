create or replace view analytics.user_trans as 
with user_trans as 
(select act.user_id,txn.customer_type, act.country, act.network_name,act.activity,act.start_at::timestamp as activity_at,act.end_at::timestamp as attribution_end_at,txn.bi_revenue_usd,txn.order_at::timestamp
from
(select activity_at as start_at, user_id, activity, 
case when country = 'ae' then 'AE' 
when country = 'bh' then 'BH'
when country = 'kw' then 'KW'
when country = 'om' then 'OM'
when country = 'qa' then 'QA'
when country = 'sa' then 'SA' else 'Rest' end as country,
case when network_name = 'Admitad KSA' then 'Admit Ad'
 when network_name = 'Admitad UAE' then 'Admit Ad'
 when network_name = 'Google Ads Search' then 'SEM'
 when network_name = 'Adwords (unknown)' then 'SEM'
 when network_name = 'Google Ads UAC' then 'Google UAC'
 when network_name = 'Google Ads Video' then 'Branding - Youtube'
 when network_name = 'Google Ads Display' then 'Google Display'
 when network_name = 'Adwords Display Installs' then 'Google Display'
 when network_name = 'Adwords iOS mGDN - Quay' then 'Google Display'
 when network_name = 'Adwords iOS Search' then 'SEM'
 when network_name = 'Adwords iOS Search All' then 'SEM'
 when network_name = 'Adwords Search Installs' then 'SEM'
 when network_name = 'Adwords UAC Installs' then 'Google UAC'
 when network_name = 'Adwords Video Installs' then 'Branding - Youtube'
 when network_name = 'Affle KSA' then 'Affle'
 when network_name = 'Affle KW' then 'Affle'
 when network_name = 'Affle QAR' then 'Affle'
 when network_name = 'Affle UAE' then 'Affle'
 when network_name = 'Apple Search Ads' then 'Apple Search Ads'
 when network_name = 'Appnlab UAE' then 'Appnlab'
 when network_name = 'ArabyAds KSA' then 'ArabyAds'
 when network_name = 'Boosted Post Insta' then 'Branding - Instagram'
 when network_name = 'Criteo' then 'Criteo'
 when network_name = 'Criteo Installs' then 'Criteo'
 when network_name = 'DoubleClick Installs' then 'Doubleclick'
 when network_name = 'Evolve KSA' then 'Evolve'
 when network_name = 'Facebook - Quay Australia' then 'Facebook'
 when network_name = 'Facebook Installs' then 'Facebook'
 when network_name = 'Facebook Messenger Installs' then 'Facebook'
 when network_name = 'Google Organic Search' then 'Organic'
 when network_name = 'Organic' then 'Organic'
 when network_name = 'YouTube Masthead' then 'Others'
 when network_name = 'Universal' then 'Others'
 when network_name = 'Test' then 'Others'
 when network_name = 'Snapchat - expanded targeting (formerly ak test)' then 'Snapchat'
 when network_name = 'Unattributed' then 'Others'
 when network_name = 'Test - Snap - Quay Australia New Category' then 'Others'
 when network_name = 'Insta|Boosted Post[clarins-value-set]' then 'Instagram'
 when network_name = 'Instagram Installs' then 'Instagram'
 when network_name = 'Liftoff KSA' then 'Liftoff'
 when network_name = 'Newsletter' then 'Newsletter'
 when network_name = 'Off-Facebook Installs' then 'Facebook'
 when network_name = 'Snapchat - Quay Australia' then 'Snapchat'
 when network_name = 'Snapchat Installs' then 'Snapchat'
 when network_name = 'Taptica KSA' then 'Taptica'
 when network_name = 'Taptica KW' then 'Taptica'
 when network_name = 'Taptica QAR' then 'Taptica'
 when network_name = 'Taptica UAE' then 'Taptica'
 when network_name = 'Twitter Installs' then 'Twitter'
 when network_name = 'Tyroo KSA' then 'Tyroo'
 when network_name = 'Tyroo KW' then 'Tyroo'
 when network_name = 'Tyroo UAE' then 'Tyroo'
 when network_name = 'Untrusted Devices' then 'Others'
 when network_name = 'Webpals KSA' then 'Webpals'
 when network_name = 'Webpals KW' then 'Webpals'
 when network_name = 'Webpals QAR' then 'Webpals'
 when network_name = 'Webpals UAE' then 'Webpals' else network_name end as network_name,
COALESCE (LEAD(activity_at,1) over (partition by user_id order by activity_at asc) - interval '30 seconds',cast('3020-01-01 00:00:00' as timestamp)) as end_at
from adjust.attributes_actions
where activity in ('install','reinstall','reattribution_reinstall')
order by activity_at asc) act 
left join (select txn.user_id, cust_type.customer_type,txn.order_at, txn.bi_revenue_usd from sandbox.transactions_201920 txn
left join (select  user_id,tx_unique_id,case when customer_type like '%new%' then 1 else 0 end as customer_type from sandbox.tx_attr_7days_201920_extended_tagged) cust_type
on txn.user_id = cust_type.user_id and txn.tx_unique_id = cust_type.tx_unique_id 
group by 1,2,3,4) txn on txn.user_id = act.user_id and txn.order_at::timestamp between act.start_at::timestamp  and act.end_at::timestamp 
order by act.start_at asc)
select * from user_trans
with no schema binding;