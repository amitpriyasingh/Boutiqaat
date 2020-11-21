BEGIN;
DELETE FROM sandbox.customer_retention WHERE 1=1;

INSERT INTO sandbox.customer_retention
with fist_last_order_country as
(
    select 
        phone_no, 
        order_currency, 
        order_at,order_date,
        min(COALESCE(order_at,order_date)) over (partition by phone_no) as first_order_at,
        max(COALESCE(order_at,order_date)) over (partition by phone_no) as last_order_at
    from aoi.order_items 
    where lower(order_category) != 'celebrity'
),
most_orders_country as 
(
    select 
        phone_no,
        order_currency, 
        count(order_number) as total_countrywise_orders,
        DENSE_RANK () over (partition by phone_no order by count(order_number) desc) as max_order_country_rank
    from aoi.order_items 
    where order_status not like '%Cancel%' and order_status not like '%Ret%' and lower(order_category) != 'celebrity'
    group by 1,2 
),
most_shopped_category1 as 
(
    select 
        phone_no, 
        category1,
        count(order_number) as total_category1_orders,
        DENSE_RANK () over (partition by phone_no order by count(order_number) desc) as max_order_cat1_rank 
    from aoi.order_items 
    where lower(order_category) != 'celebrity' 
    group by 1,2
),
most_shopped_category2 as 
(
    select 
        phone_no, 
        category2,
        count(order_number) as total_category2_orders,
        DENSE_RANK () over (partition by phone_no order by count(order_number) desc) as max_order_cat2_rank 
    from aoi.order_items 
    where lower(order_category) != 'celebrity' 
    group by 1,2
),
orders as 
(
    select 
        phone_no, 
        count(distinct case when COALESCE(order_at,order_date) <= getdate() and COALESCE(order_at,order_date) >= dateadd(day,-1,getdate()) then order_number else null end) as orders_1day,
        count(distinct case when COALESCE(order_at,order_date) <= getdate() and COALESCE(order_at,order_date) >= dateadd(day,-2,getdate()) then order_number else null end) as orders_2days,
        count(distinct case when COALESCE(order_at,order_date) <= getdate() and COALESCE(order_at,order_date) >= dateadd(day,-3,getdate()) then order_number else null end) as orders_3days,
        count(distinct case when COALESCE(order_at,order_date) <= getdate() and COALESCE(order_at,order_date) >= dateadd(day,-5,getdate()) then order_number else null end) as orders_5days,
        count(distinct case when COALESCE(order_at,order_date) <= getdate() and COALESCE(order_at,order_date) >= dateadd(day,-10,getdate()) then order_number else null end) as orders_10days,
        count(distinct case when COALESCE(order_at,order_date) <= getdate() and COALESCE(order_at,order_date) >= dateadd(day,-20,getdate()) then order_number else null end) as orders_20days,
        count(distinct case when COALESCE(order_at,order_date) <= getdate() and COALESCE(order_at,order_date) >= dateadd(day,-30,getdate()) then order_number else null end) as orders_30days,
        count(distinct case when COALESCE(order_at,order_date) <= getdate() and COALESCE(order_at,order_date) >= dateadd(day,-60,getdate()) then order_number else null end) as orders_60days,
        count(distinct case when COALESCE(order_at,order_date) <= getdate() and COALESCE(order_at,order_date) >= dateadd(day,-90,getdate()) then order_number else null end) as orders_90days,
        count(distinct case when COALESCE(order_at,order_date) <= dateadd(day,-3,getdate()) and COALESCE(order_at,order_date) >= dateadd(day,-5,getdate()) then order_number else null end) as orders_3days_5days,
        count(distinct case when COALESCE(order_at,order_date) <= dateadd(day,-5,getdate()) and COALESCE(order_at,order_date) >= dateadd(day,-10,getdate()) then order_number else null end) as orders_5days_10days,
        count(distinct case when COALESCE(order_at,order_date) <= dateadd(day,-10,getdate()) and COALESCE(order_at,order_date) >= dateadd(day,-100,getdate()) then order_number else null end) as orders_10days_100days,
        count(distinct case when COALESCE(order_at,order_date) <= dateadd(day,-20,getdate()) and COALESCE(order_at,order_date) >= dateadd(day,-110,getdate()) then order_number else null end) as orders_20days_110days,
        count(distinct case when COALESCE(order_at,order_date) <= dateadd(day,-30,getdate()) and COALESCE(order_at,order_date) >= dateadd(day,-120,getdate()) then order_number else null end) as orders_30days_120days,
        count(distinct case when COALESCE(order_at,order_date) <= getdate() and COALESCE(order_at,order_date) >= dateadd(day,-180,getdate()) then order_number else null end) as orders_180days,
        count(distinct case when COALESCE(order_at,order_date) <= dateadd(day,-10,getdate()) and COALESCE(order_at,order_date) >= dateadd(day,-190,getdate()) then order_number else null end) as orders_10days_190days,
        count(distinct case when COALESCE(order_at,order_date) <= dateadd(day,-20,getdate()) and COALESCE(order_at,order_date) >= dateadd(day,-200,getdate()) then order_number else null end) as orders_20days_200days,
        count(distinct case when COALESCE(order_at,order_date) <= dateadd(day,-30,getdate()) and COALESCE(order_at,order_date) >= dateadd(day,-210,getdate()) then order_number else null end) as orders_30days_210days,
        count(distinct case when COALESCE(order_at,order_date) <= dateadd(day,-100,getdate()) and COALESCE(order_at,order_date) >= dateadd(day,-190,getdate()) then order_number else null end) as orders_100days_190days,
        count(distinct case when COALESCE(order_at,order_date) <= dateadd(day,-110,getdate()) and COALESCE(order_at,order_date) >= dateadd(day,-200,getdate()) then order_number else null end) as orders_110days_200days,
        count(distinct case when COALESCE(order_at,order_date) <= dateadd(day,-120,getdate()) and COALESCE(order_at,order_date) >= dateadd(day,-210,getdate()) then order_number else null end) as orders_120days_210days,
        count(distinct case when COALESCE(order_at,order_date) <= getdate() and COALESCE(order_at,order_date) >= dateadd(day,-365,getdate()) then order_number else null end) as orders_365days,
        count(case when EXTRACT(month from COALESCE(order_at,order_date))= EXTRACT(month from getdate()) and EXTRACT(year from COALESCE(order_at,order_date)) = EXTRACT(year from getdate()) then order_number else null end) as orders_this_month,
        count(distinct 
	            case 
		            when 
                        EXTRACT(month from COALESCE(order_at,order_date)) = (case when EXTRACT(month from getdate())-1 = 0 
                                        then 12 else EXTRACT(month from getdate())-1 end) 
                        and 
                        (case when EXTRACT(month from getdate())-1 = 0 then EXTRACT(year from getdate())-1 else EXTRACT(year from getdate()) end) = EXTRACT(year from COALESCE(order_at,order_date)) 
                    then order_number else null 
                end
	    ) as orders_last_month,
        count(distinct 
            case 
                when 
                    EXTRACT(month from COALESCE(order_at,order_date)) = (case when EXTRACT(month from getdate())-2 = 0 
                                    then 12 else EXTRACT(month from getdate())-2 end) 
                    and 
                    (case when EXTRACT(month from getdate())-2 = 0 then EXTRACT(year from getdate())-1 else EXTRACT(year from getdate()) end) = EXTRACT(year from COALESCE(order_at,order_date)) 
                    then order_number
                when 
                    EXTRACT(month from COALESCE(order_at,order_date)) = (case when EXTRACT(month from getdate())-2 = -1 
                                    then 11 else EXTRACT(month from getdate())-2 end) 
                    and 
                    (case when EXTRACT(month from getdate())-2 = -1 then EXTRACT(year from getdate())-1 else EXTRACT(year from getdate()) end) = EXTRACT(year from COALESCE(order_at,order_date))  
                    then order_number else null
            end
            ) as orders_second_last_month,
        count(distinct 
            case 
                when 
                    EXTRACT(month from COALESCE(order_at,order_date)) = (case when EXTRACT(month from getdate())-3 = 0 
                                    then 12 else EXTRACT(month from getdate())-3 end) 
                    and 
                    (case when EXTRACT(month from getdate())-3 = 0 then EXTRACT(year from getdate())-1 else EXTRACT(year from getdate()) end) = EXTRACT(year from COALESCE(order_at,order_date)) 
                    then order_number
                when 
                    EXTRACT(month from COALESCE(order_at,order_date)) = (case when EXTRACT(month from getdate())-3 = -1 
                                    then 11 else EXTRACT(month from getdate())-3 end) 
                    and 
                    (case when EXTRACT(month from getdate())-3 = -1 then EXTRACT(year from getdate())-1 else EXTRACT(year from getdate()) end) = EXTRACT(year from COALESCE(order_at,order_date))  
                    then order_number
                when 
                    EXTRACT(month from COALESCE(order_at,order_date)) = (case when EXTRACT(month from getdate())-3 = -2 
                                    then 10 else EXTRACT(month from getdate())-3 end) 
                    and 
                    (case when EXTRACT(month from getdate())-3 = -1 then EXTRACT(year from getdate())-1 else EXTRACT(year from getdate()) end) = EXTRACT(year from COALESCE(order_at,order_date))  
                    then order_number else null
            end
            ) as orders_third_last_month,
        count(distinct case when COALESCE(order_at,order_date)<date_trunc('month',getdate()) and COALESCE(order_at,order_date)>(date_trunc('month',getdate()) - interval '180 days') then order_number else null end) as orders_180days_prior_to_this_month,
        count(distinct case when COALESCE(order_at,order_date)<add_months(date_trunc('month',getdate()),-1) and COALESCE(order_at,order_date)>(add_months(date_trunc('month',getdate()),-1) - interval '180 day') then order_number else null end) as orders_180days_prior_to_last_month,
        count(distinct case when COALESCE(order_at,order_date)<add_months(date_trunc('month',getdate()),-2) and COALESCE(order_at,order_date)>(add_months(date_trunc('month',getdate()),-2) - interval '180 day') then order_number else null end) as orders_180days_prior_to_second_last_month,
        count(distinct case when COALESCE(order_at,order_date)<add_months(date_trunc('month',getdate()),-3) and COALESCE(order_at,order_date)>(add_months(date_trunc('month',getdate()),-3) - interval '180 day') then order_number else null end) as orders_180days_prior_to_third_last_month,
        count(distinct case when COALESCE(order_at,order_date)<date_trunc('month',getdate()) and COALESCE(order_at,order_date)>(date_trunc('month',getdate()) - interval '90 days') then order_number else null end) as orders_90days_prior_to_this_month,
        count(distinct case when COALESCE(order_at,order_date)<add_months(date_trunc('month',getdate()),-1) and COALESCE(order_at,order_date)>(add_months(date_trunc('month',getdate()),-1) - interval '90 day') then order_number else null end) as orders_90days_prior_to_last_month,
        count(distinct case when COALESCE(order_at,order_date)<add_months(date_trunc('month',getdate()),-2) and COALESCE(order_at,order_date)>(add_months(date_trunc('month',getdate()),-2) - interval '90 day') then order_number else null end) as orders_90days_prior_to_second_last_month,
        count(distinct case when COALESCE(order_at,order_date)<add_months(date_trunc('month',getdate()),-3) and COALESCE(order_at,order_date)>(add_months(date_trunc('month',getdate()),-3) - interval '90 day') then order_number else null end) as orders_90days_prior_to_third_last_month,
        count(distinct order_number) as total_orders_till_date,
        count(distinct case when COALESCE(order_at,order_date) < dateadd(day,-30,getdate()) then order_number else null end) as orders_greater_than_30days,
        sum(case when COALESCE(order_at,order_date) <= getdate() and COALESCE(order_at,order_date) >= dateadd(day,-10,getdate()) then (net_sale_price_kwd + shipping_charge_kwd + cod_charge_kwd) else 0 end) as grs_rev_10days,
        sum(case when COALESCE(order_at,order_date) <= getdate() and COALESCE(order_at,order_date) >= dateadd(day,-20,getdate()) then (net_sale_price_kwd + shipping_charge_kwd + cod_charge_kwd) else 0 end) as grs_rev_20days,
        sum(case when COALESCE(order_at,order_date) <= getdate() and COALESCE(order_at,order_date) >= dateadd(day,-30,getdate()) then (net_sale_price_kwd + shipping_charge_kwd + cod_charge_kwd) else 0 end) as grs_rev_30days,
        sum(case when COALESCE(order_at,order_date) <= getdate() and COALESCE(order_at,order_date) >= dateadd(day,-60,getdate()) then (net_sale_price_kwd + shipping_charge_kwd + cod_charge_kwd) else 0 end) as grs_rev_60days,
        sum(case when COALESCE(order_at,order_date) <= getdate() and COALESCE(order_at,order_date) >= dateadd(day,-90,getdate()) then (net_sale_price_kwd + shipping_charge_kwd + cod_charge_kwd) else 0 end) as grs_rev_90days,
        sum(case when COALESCE(order_at,order_date) <= getdate() and COALESCE(order_at,order_date) >= dateadd(day,-180,getdate()) then (net_sale_price_kwd + shipping_charge_kwd + cod_charge_kwd) else 0 end) as grs_rev_180days,
        sum(case when COALESCE(order_at,order_date) <= getdate() and COALESCE(order_at,order_date) >= dateadd(day,-365,getdate()) then (net_sale_price_kwd + shipping_charge_kwd + cod_charge_kwd) else 0 end) as grs_rev_365days,
        sum(COALESCE(net_sale_price_kwd,0) + COALESCE(shipping_charge_kwd,0) + COALESCE(cod_charge_kwd,0)) as gross_revenue_life_time,
        sum(case when order_status not like '%Cancel%' and order_status not like '%Ret%' and COALESCE(order_at,order_date) <= getdate() and COALESCE(order_at,order_date) >= dateadd(day,-10,getdate()) then (net_sale_price_kwd + shipping_charge_kwd + cod_charge_kwd) else 0 end) as grs_rev_10days_exclude_can_ret,
        sum(case when order_status not like '%Cancel%' and order_status not like '%Ret%' and COALESCE(order_at,order_date) <= getdate() and COALESCE(order_at,order_date) >= dateadd(day,-20,getdate()) then (net_sale_price_kwd + shipping_charge_kwd + cod_charge_kwd) else 0 end) as grs_rev_20days_exclude_can_ret,
        sum(case when order_status not like '%Cancel%' and order_status not like '%Ret%' and COALESCE(order_at,order_date) <= getdate() and COALESCE(order_at,order_date) >= dateadd(day,-30,getdate()) then (net_sale_price_kwd + shipping_charge_kwd + cod_charge_kwd) else 0 end) as grs_rev_30days_exclude_can_ret,
        sum(case when order_status not like '%Cancel%' and order_status not like '%Ret%' and COALESCE(order_at,order_date) <= getdate() and COALESCE(order_at,order_date) >= dateadd(day,-60,getdate()) then (net_sale_price_kwd + shipping_charge_kwd + cod_charge_kwd) else 0 end) as grs_rev_60days_exclude_can_ret,
        sum(case when order_status not like '%Cancel%' and order_status not like '%Ret%'  and COALESCE(order_at,order_date) <= getdate() and COALESCE(order_at,order_date) >= dateadd(day,-90,getdate()) then (net_sale_price_kwd + shipping_charge_kwd + cod_charge_kwd) else 0 end) as grs_rev_90days_exclude_can_ret,
        sum(case when order_status not like '%Cancel%' and order_status not like '%Ret%'  and COALESCE(order_at,order_date) <= getdate() and COALESCE(order_at,order_date) >= dateadd(day,-180,getdate()) then (net_sale_price_kwd + shipping_charge_kwd + cod_charge_kwd) else 0 end) as grs_rev_180days_exclude_can_ret,
        sum(case when order_status not like '%Cancel%' and order_status not like '%Ret%'  and COALESCE(order_at,order_date) <= getdate() and COALESCE(order_at,order_date) >= dateadd(day,-365,getdate()) then (net_sale_price_kwd + shipping_charge_kwd + cod_charge_kwd) else 0 end) as grs_rev_365days_exclude_can_ret,
        sum(case when order_status not like '%Cancel%' and order_status not like '%Ret%'  then (net_sale_price_kwd + shipping_charge_kwd + cod_charge_kwd) else 0 end) as grs_rev_lifetime_exclude_can_ret
    from aoi.order_items 
    where lower(order_category) != 'celebrity' 
    group by 1
),
transactions as 
(
    select 
        distinct trans.phone_no, 
        COALESCE (trans.order_at,trans.order_date) as order_at,
        count(1) over (partition by trans.phone_no) as total_count,
        DENSE_RANK() over (partition by trans.phone_no order by COALESCE (trans.order_at,trans.order_date) asc) as trans_rank,
        LEAD(COALESCE (trans.order_at,trans.order_date),1) over (partition by trans.phone_no order by COALESCE (trans.order_at,trans.order_date) asc) as days_gap
    from 
    (
        select 
            distinct phone_no, 
            order_at,
            order_date 
        from aoi.order_items 
        where lower(order_category) != 'celebrity'
    ) trans --where trans.phone_no is not null and trans.order_at is not null
    order by trans.phone_no
)-----------------
select 
    i.phone_no,foc.acquisition_date,foc.last_transaction_date,
    case when charindex(',',foc.first_order_country) != 0 then SUBSTRING(foc.first_order_country,1,charindex(',',foc.first_order_country)-1) else foc.first_order_country end as first_order_country, 
    case when charindex(',',loc.last_order_country) != 0 then SUBSTRING(loc.last_order_country,1,charindex(',',loc.last_order_country)-1) else loc.last_order_country end as last_order_country,
    case when charindex(',',moc.most_order_country) != 0 then SUBSTRING(moc.most_order_country,1,charindex(',',moc.most_order_country)-1) else moc.most_order_country end as most_order_country, 
    case when charindex(',',most_cat1.most_shopped_cat1) != 0 then SUBSTRING(most_cat1.most_shopped_cat1,1,charindex(',',most_cat1.most_shopped_cat1)-1) else most_cat1.most_shopped_cat1 end as most_shopped_cat1, 
    case when charindex(',',second_most_cat1.second_most_shopped_cat1) != 0 then SUBSTRING(second_most_cat1.second_most_shopped_cat1,1,charindex(',',second_most_cat1.second_most_shopped_cat1)-1) else second_most_cat1.second_most_shopped_cat1 end as second_most_shopped_cat1, 
    case when charindex(',',most_cat2.most_shopped_cat2) != 0 then SUBSTRING(most_cat2.most_shopped_cat2,1,charindex(',',most_cat2.most_shopped_cat2)-1) else most_cat2.most_shopped_cat2 end as most_shopped_cat2,  
    case when charindex(',',second_most_cat2.second_most_shopped_cat2) != 0 then SUBSTRING(second_most_cat2.second_most_shopped_cat2,1,charindex(',',second_most_cat2.second_most_shopped_cat2)-1) else second_most_cat2.second_most_shopped_cat2 end as second_most_shopped_cat2, 
    orders.orders_1day,
    orders.orders_2days, 
    orders.orders_3days, 
    orders.orders_5days, 
    orders.orders_3days_5days, 
    orders.orders_5days_10days,
    orders.orders_10days,
    orders.orders_20days,
    orders.orders_30days,
    orders.orders_60days, 
    orders.orders_90days,
    orders.orders_greater_than_30days,
    orders.orders_10days_100days,
    orders.orders_20days_110days,
    orders.orders_30days_120days,
    orders.orders_180days, 
    orders.orders_10days_190days,
    orders.orders_20days_200days,
    orders.orders_30days_210days,
    orders.orders_365days, 
    orders.orders_100days_190days, 
    orders.orders_110days_200days, 
    orders.orders_120days_210days,
    orders.orders_this_month,
    orders.orders_last_month,
    orders.orders_second_last_month,
    orders.orders_third_last_month,
    orders.orders_180days_prior_to_this_month,
    orders.orders_180days_prior_to_last_month,
    orders.orders_180days_prior_to_second_last_month,
    orders.orders_180days_prior_to_third_last_month,
    orders.orders_90days_prior_to_this_month,
    orders.orders_90days_prior_to_last_month,
    orders.orders_90days_prior_to_second_last_month,
    orders.orders_90days_prior_to_third_last_month,
    orders.grs_rev_10days,
    orders.grs_rev_20days,
    orders.grs_rev_30days,
    orders.grs_rev_60days,
    orders.grs_rev_90days,
    orders.grs_rev_180days,
    orders.grs_rev_365days,
    orders.gross_revenue_life_time,
    orders.grs_rev_10days_exclude_can_ret,
    orders.grs_rev_20days_exclude_can_ret,
    orders.grs_rev_30days_exclude_can_ret,
    orders.grs_rev_60days_exclude_can_ret,
    orders.grs_rev_90days_exclude_can_ret,
    orders.grs_rev_180days_exclude_can_ret,
    orders.grs_rev_365days_exclude_can_ret,
    orders.grs_rev_lifetime_exclude_can_ret,
    (gap_first_trans.days_gap::date - gap_first_trans.order_at::date) as gap_btw_1st_2nd_trans,
    (gap_second_trans.days_gap::date - gap_second_trans.order_at::date) as gap_btw_2nd_3rd_trans,
    (gap_third_trans.days_gap::date - gap_third_trans.order_at::date) as gap_btw_3rd_4th_trans,
    (gap_fourth_trans.days_gap::date - gap_fourth_trans.order_at::date) as gap_btw_4th_5th_trans,
    (getdate()::date - gap_last_trans.order_at::date) as gap_btw_since_last_trans,
    (gap_second_last_trans.days_gap::date - gap_second_last_trans.order_at::date) as gap_btw_last_2nd_last_trans,
    (gap_2nd_last_3rd_last_trans.days_gap::date - gap_2nd_last_3rd_last_trans.order_at::date) as gap_btw_2nd_last_3rd_last_trans,
    orders.total_orders_till_date, 
    rev_rank.cust_rev_rank,
    (max(rev_rank.cust_rev_rank) over ()) as max_rank, 
    (max(rev_rank.cust_rev_rank) over ()-rev_rank.cust_rev_rank)*100/(max(rev_rank.cust_rev_rank) over ())::decimal as rev_percentile
from 
(
    select 
        distinct phone_no 
    from aoi.order_items
) i
left join 
(
    select 
        distinct phone_no, 
        first_order_at::date as acquisition_date, 
        last_order_at::date as last_transaction_date,
        listagg(distinct order_currency,',') as first_order_country
    from fist_last_order_country 
    where first_order_at = COALESCE(order_at,order_date) 
    group by 1,2,3
) foc on foc.phone_no = i.phone_no
left join 
(
    select 
        distinct phone_no, 
        listagg(distinct order_currency,',') as last_order_country 
    from fist_last_order_country 
    where last_order_at = COALESCE(order_at,order_date) 
    group by 1
) as loc on loc.phone_no = i.phone_no
left join 
(
    select 
        distinct phone_no, 
        listagg(order_currency,',') as most_order_country 
    from most_orders_country 
    where max_order_country_rank = 1 
    group by 1
) as moc on moc.phone_no = i.phone_no
left join 
(
    select 
        phone_no,
        listagg(distinct category1,',') as most_shopped_cat1
    from most_shopped_category1 
    where max_order_cat1_rank = 1 
    group by 1 
) as most_cat1 on most_cat1.phone_no = i.phone_no
left join 
(
    select 
        phone_no,
        listagg(distinct category1,',') as second_most_shopped_cat1
    from most_shopped_category1 
    where max_order_cat1_rank = 2 
    group by 1 
) as second_most_cat1 on second_most_cat1.phone_no = i.phone_no
left join 
(
    select 
        phone_no,
        listagg(category2,',') as most_shopped_cat2
    from most_shopped_category2 
    where max_order_cat2_rank = 1
    group by 1
) as most_cat2 on most_cat2.phone_no = i.phone_no
left join 
(
    select 
        phone_no,
        listagg(category2,',') as second_most_shopped_cat2
    from most_shopped_category2 
    where max_order_cat2_rank = 2 
    group by 1
) as second_most_cat2 on second_most_cat2.phone_no = i.phone_no
left join orders on orders.phone_no = i.phone_no
left join 
(
    select 
        phone_no, 
        gross_revenue_life_time, 
        RANK() over (order by gross_revenue_life_time desc) as cust_rev_rank
    from orders
) as rev_rank on rev_rank.phone_no = i.phone_no
left join 
(
    select 
        distinct phone_no,
        order_at,
        days_gap 
    from transactions 
    where trans_rank = 1
) as gap_first_trans on gap_first_trans.phone_no = i.phone_no
left join 
(
    select 
        distinct phone_no,
        order_at,
        days_gap 
    from transactions 
    where trans_rank = 2
) as gap_second_trans on gap_second_trans.phone_no = i.phone_no
left join 
(
    select 
        distinct phone_no,
        order_at,
        days_gap 
    from transactions 
    where trans_rank = 3
) as gap_third_trans on gap_third_trans.phone_no = i.phone_no
left join 
(
    select 
        distinct phone_no,
        order_at,
        days_gap 
    from transactions 
    where trans_rank = 4
) as gap_fourth_trans on gap_fourth_trans.phone_no = i.phone_no
left join 
(
    select 
        distinct phone_no,
        order_at,
        days_gap 
    from transactions 
    where trans_rank = total_count
) as gap_last_trans on gap_last_trans.phone_no = i.phone_no
left join 
(
    select 
        distinct phone_no,
        order_at,
        days_gap 
    from transactions 
    where trans_rank = total_count-1
) as gap_second_last_trans on gap_second_last_trans.phone_no = i.phone_no
left join 
(
    select 
        distinct phone_no,
        order_at,
        days_gap 
    from transactions 
    where trans_rank = total_count-2
) as gap_2nd_last_3rd_last_trans on gap_2nd_last_3rd_last_trans.phone_no = i.phone_no;

COMMIT;
