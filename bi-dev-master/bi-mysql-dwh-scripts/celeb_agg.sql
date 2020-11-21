
#aoi_agg
#=======
select max(order_date) into @last_order_date from aoi_agg;

#Add all gift orders of Yesterday
DELETE FROM aoi_gift_agg WHERE order_date >= @last_order_date;
insert into aoi_gift_agg SELECT order_date order_date, celebrity_id ,Count(DISTINCT order_number) gift_orders, Sum(item_total) gift_item_total, Sum(quantity*item_cost) gift_cogs FROM aoi WHERE country="Celebrities" and order_date >= @last_order_date GROUP BY 1,2;

#Add all non-gift orders of Yesterday
DELETE FROM aoi_non_gift_agg WHERE order_date >= @last_order_date;
insert into aoi_non_gift_agg
SELECT DATE(aoi.order_date) order_date, aoi.celebrity_id, celebrity_name, Count(DISTINCT order_number) total_orders, Count(DISTINCT IF(category1="Makeup",order_number,NULL)) makeup_orders, Count(DISTINCT IF(category1="Skin Care",order_number,NULL)) skincare_orders, Count(DISTINCT IF(category1="Fragrances",order_number,NULL)) fragrances_orders, Count(DISTINCT IF(category1="Hair Care",order_number,NULL)) haircare_orders, Count(DISTINCT IF(category1="Bath & Body",order_number,NULL)) bath_orders, Count(DISTINCT IF(category1="Fashion",order_number,NULL)) fashion_orders, Count(DISTINCT IF(category1="Eyewear",order_number,NULL)) eyewear_orders, Count(DISTINCT IF(category1="Fashion Tech",order_number,NULL)) fashiontech_orders, Count(DISTINCT IF(category1="Watches",order_number,NULL)) watches_orders, Count(DISTINCT IF(category1="Beauty",order_number,NULL)) beauty_orders, Sum(quantity*item_cost) total_cogs, Sum(item_total) total_rev, Sum(IF(category1="Makeup",item_total,NULL)) makeup_rev, Sum(IF(category1="Skin Care",item_total,NULL)) skincare_rev, Sum(IF(category1="Fragrances",item_total,NULL)) fragrances_rev, Sum(IF(category1="Hair Care",item_total,NULL)) haircare_rev, Sum(IF(category1="Bath & Body",item_total,NULL)) bath_rev, Sum(IF(category1="Fashion",item_total,NULL)) fashion_rev, Sum(IF(category1="Eyewear",item_total,NULL)) eyewear_rev, Sum(IF(category1="Fashion Tech",item_total,NULL)) fashiontech_rev, Sum(IF(category1="Watches",item_total,NULL)) watches_rev, Sum(IF(category1="Beauty",item_total,NULL)) beauty_rev, Sum(quantity) total_qty, Sum(IF(category1="Makeup",quantity,NULL)) makeup_qty, Sum(IF(category1="Skin Care",quantity,NULL)) skincare_qty, Sum(IF(category1="Fragrances",quantity,NULL)) fragrances_qty, Sum(IF(category1="Hair Care",quantity,NULL)) haircare_qty, Sum(IF(category1="Bath & Body",quantity,NULL)) bath_qty, Sum(IF(category1="Fashion",quantity,NULL)) fashion_qty, Sum(IF(category1="Eyewear",quantity,NULL)) eyewear_qty, Sum(IF(category1="Fashion Tech",quantity,NULL)) fashiontech_qty, Sum(IF(category1="Watches",quantity,NULL)) watches_qty, Sum(IF(category1="Beauty",quantity,NULL)) beauty_qty, Sum((item_total - quantity*item_cost)) total_margin, Sum(IF(category1="Makeup",(item_total - quantity*item_cost),NULL)) makeup_margin, Sum(IF(category1="Skin Care",(item_total - quantity*item_cost),NULL)) skincare_margin, Sum(IF(category1="Fragrances",(item_total - quantity*item_cost),NULL)) fragrances_margin, Sum(IF(category1="Hair Care",(item_total - quantity*item_cost),NULL)) haircare_margin, Sum(IF(category1="Bath & Body",(item_total - quantity*item_cost),NULL)) bath_margin, Sum(IF(category1="Fashion",(item_total - quantity*item_cost),NULL)) fashion_margin, Sum(IF(category1="Eyewear",(item_total - quantity*item_cost),NULL)) eyewear_margin, Sum(IF(category1="Fashion Tech",(item_total - quantity*item_cost),NULL)) fashiontech_margin, Sum(IF(category1="Watches",(item_total - quantity*item_cost),NULL)) watches_margin, Sum(IF(category1="Beauty",(item_total - quantity*item_cost),NULL)) beauty_margin, Sum(celebrity_commission) celebrity_commission FROM aoi where aoi.country <> "Celebrities" and order_date >= @last_order_date group by 1,2;

#Add all gift & non-gift orders of Yesterday
DELETE FROM aoi_agg WHERE order_date >= @last_order_date;
insert into aoi_agg
select ng.order_date order_date, ng.celebrity_id, celebrity_name, total_orders, makeup_orders, skincare_orders, fragrances_orders, haircare_orders, bath_orders, fashion_orders, eyewear_orders, fashiontech_orders, watches_orders, beauty_orders, total_cogs, total_rev, makeup_rev, skincare_rev, fragrances_rev, haircare_rev, bath_rev, fashion_rev, eyewear_rev, fashiontech_rev, watches_rev, beauty_rev, total_qty, makeup_qty, skincare_qty, fragrances_qty, haircare_qty, bath_qty, fashion_qty, eyewear_qty, fashiontech_qty, watches_qty, beauty_qty, total_margin, makeup_margin, skincare_margin, fragrances_margin, haircare_margin, bath_margin, fashion_margin, eyewear_margin, fashiontech_margin, watches_margin, beauty_margin, celebrity_commission, g.gift_orders,  g.gift_item_total, g.gift_cogs from aoi_non_gift_agg ng LEFT JOIN aoi_gift_agg g on ng.celebrity_id=g.celebrity_id AND ng.order_date=g.order_date where ng.order_date >= @last_order_date
UNION ALL
select g.order_date order_date, g.celebrity_id, celebrity_name, total_orders, makeup_orders, skincare_orders, fragrances_orders, haircare_orders, bath_orders, fashion_orders, eyewear_orders, fashiontech_orders, watches_orders, beauty_orders, total_cogs, total_rev, makeup_rev, skincare_rev, fragrances_rev, haircare_rev, bath_rev, fashion_rev, eyewear_rev, fashiontech_rev, watches_rev, beauty_rev, total_qty, makeup_qty, skincare_qty, fragrances_qty, haircare_qty, bath_qty, fashion_qty, eyewear_qty, fashiontech_qty, watches_qty, beauty_qty, total_margin, makeup_margin, skincare_margin, fragrances_margin, haircare_margin, bath_margin, fashion_margin, eyewear_margin, fashiontech_margin, watches_margin, beauty_margin, celebrity_commission, g.gift_orders,  g.gift_item_total, g.gift_cogs from aoi_gift_agg g LEFT JOIN aoi_non_gift_agg ng on ng.celebrity_id=g.celebrity_id AND ng.order_date=g.order_date WHERE ng.celebrity_id IS NULL AND g.order_date >= @last_order_date;

#event_agg
#=========
select max(event_date) into @max_event_date from events_agg;
DELETE FROM events_agg WHERE event_date >= @max_event_date;
INSERT INTO events_agg 
            (event_date,              celebrity_id,              celebrity_name,              account_manager,				 bq_post, 			total_post,            total_events,              unique_sku,              makeup_events,              skincare_events,              fragrances_events,              haircare_events,              bath_events,              fashion_events,              eyewear_events,              fashiontech_events,              watches_events,              beauty_events,              snapchat_events,              instagram_events,              others_events,              instastories_events,              igtv_events, 
            youtube_events) 
SELECT event_date, 
       celebrity_id, 
       celebrity_name, 
       account_manager, 
       SUM(alloc_bq_post),
       SUM(total_post),
       Count(DISTINCT event_id) 
       total_events, 
       Count(DISTINCT productid)         unique_sku, 
       Count(DISTINCT IF(category1 = "makeup", event_id, NULL)) 
       makeup_events, 
       Count(DISTINCT IF(category1 = "skin care", event_id, NULL)) 
       skincare_events, 
       Count(DISTINCT IF(category1 = "fragrances", event_id, NULL)) 
       fragrances_events, 
       Count(DISTINCT IF(category1 = "hair care", event_id, NULL)) 
       haircare_events, 
       Count(DISTINCT IF(category1 = "bath & body", event_id, NULL)) 
       bath_events, 
       Count(DISTINCT IF(category1 = "fashion", event_id, NULL)) 
       fashion_events, 
       Count(DISTINCT IF(category1 = "eyewear", event_id, NULL)) 
       eyewear_events, 
       Count(DISTINCT IF(category1 = "fashion tech", event_id, NULL)) 
       fashiontech_events, 
       Count(DISTINCT IF(category1 = "watches", event_id, NULL)) 
       watches_events, 
       Count(DISTINCT IF(category1 = "beauty", event_id, NULL)) 
       beauty_events, 
       Count(DISTINCT IF(event_portal = "snapchat", event_id, NULL)) 
       snapchat_events, 
       Count(DISTINCT IF(event_portal = "instagram", event_id, NULL)) 
       instagram_events, 
       Count(DISTINCT IF(event_portal = "others", event_id, NULL)) 
       others_events, 
       Count(DISTINCT IF(event_portal = "insta stories", event_id, NULL)) 
       instastories_events, 
       Count(DISTINCT IF(event_portal = "igtv", event_id, NULL)) 
       igtv_events, 
       Count(DISTINCT IF(event_portal = "youtube", event_id, NULL)) 
       youtube_events 
FROM   events_report 
WHERE  event_date >= @max_event_date
GROUP  BY event_date, 
          celebrity_id; 


#traffic_agg
#===========
select max(traffic_date) into @max_traffic_date from traffic_agg;
DELETE FROM traffic_agg WHERE traffic_date >= @max_traffic_date;
INSERT INTO traffic_agg 
            (traffic_date, 
             celebrity_id, 
             celebrity_name, 
             primary_sessions, 
             sec_sessions, 
             total_sessions, 
             users) 
SELECT `date`                                traffic_date, 
       primary_src_id                      celebrity_id, 
       celeb_name                          celebrity_name, 
       primary_sessions, 
       sec_sessions, 
       ( primary_sessions + sec_sessions ) total_sessions, 
       users 
FROM   ga.prime_sec_traffic 
WHERE  `date` >= @max_traffic_date
GROUP  BY `date`, 
          primary_src_id; 

#celeb_agg
#==========
SELECT IF(@last_order_date < @max_event_date, IF(@last_order_date < @max_traffic_date , @last_order_date, @max_traffic_date), IF(@max_event_date < @max_traffic_date , @max_event_date, @max_traffic_date)) into @min_agg_date;

DELETE FROM celeb_agg where `date` >= @min_agg_date;

INSERT INTO celeb_agg 
(date , month , celebrity_id , celebrity_name , account_manager , bq_post , total_post , total_events , unique_sku , makeup_events , skincare_events , fragrances_events , haircare_events , bath_events , fashion_events , eyewear_events , fashiontech_events , watches_events , beauty_events , snapchat_events , instagram_events , others_events , instastories_events , igtv_events , youtube_events , total_orders , makeup_orders , skincare_orders , fragrances_orders , haircare_orders , bath_orders , fashion_orders , eyewear_orders , fashiontech_orders , watches_orders , beauty_orders , total_rev , makeup_rev , skincare_rev , fragrances_rev , haircare_rev , bath_rev , fashion_rev , eyewear_rev , fashiontech_rev , watches_rev , beauty_rev , total_qty , makeup_qty , skincare_qty , fragrances_qty , haircare_qty , bath_qty , fashion_qty , eyewear_qty , fashiontech_qty , watches_qty , beauty_qty , total_margin , makeup_margin , skincare_margin , fragrances_margin , haircare_margin , bath_margin , fashion_margin , eyewear_margin , fashiontech_margin , watches_margin , beauty_margin , celebrity_commission , gift_orders , gift_item_total , gift_cogs , primary_sessions , sec_sessions , total_sessions , users 
)
SELECT date(a.activity_date) activity_date, 
Extract(YEAR_MONTH FROM a.activity_date) activity_month, 
a.activity_celeb_id as celebrity_id, 
a.activity_celebrity_name celebrity_name, 
account_manager, 
bq_post, 
total_post, 
total_events, 
unique_sku, 
makeup_events, 
skincare_events, 
fragrances_events, 
haircare_events, 
bath_events, 
fashion_events, 
eyewear_events, 
fashiontech_events, 
watches_events, 
beauty_events, 
snapchat_events, 
instagram_events, 
others_events, 
instastories_events, 
igtv_events, 
youtube_events, 
a.total_orders, 
a.makeup_orders, 
a.skincare_orders, 
a.fragrances_orders, 
a.haircare_orders, 
a.bath_orders, 
a.fashion_orders, 
a.eyewear_orders, 
a.fashiontech_orders, 
a.watches_orders, 
a.beauty_orders, 
a.total_rev, 
a.makeup_rev, 
a.skincare_rev, 
a.fragrances_rev, 
a.haircare_rev, 
a.bath_rev, 
a.fashion_rev, 
a.eyewear_rev, 
a.fashiontech_rev, 
a.watches_rev, 
a.beauty_rev, 
a.total_qty, 
a.makeup_qty, 
a.skincare_qty, 
a.fragrances_qty, 
a.haircare_qty, 
a.bath_qty, 
a.fashion_qty, 
a.eyewear_qty, 
a.fashiontech_qty, 
a.watches_qty, 
a.beauty_qty, 
a.total_margin, 
a.makeup_margin, 
a.skincare_margin, 
a.fragrances_margin, 
a.haircare_margin, 
a.bath_margin, 
a.fashion_margin, 
a.eyewear_margin, 
a.fashiontech_margin, 
a.watches_margin, 
a.beauty_margin, 
a.celebrity_commission, 
a.gift_orders, 
a.gift_item_total, 
a.gift_cogs, 
t.primary_sessions, 
t.sec_sessions, 
t.total_sessions, 
t.users 
FROM 
(select IFNULL(a.order_date,event_date) activity_date, IFNULL(a.celebrity_id,e.celebrity_id) activity_celeb_id, IFNULL(a.celebrity_name,e.celebrity_name) activity_celebrity_name, a.*, event_date, e.celebrity_id evt_celebrity_id, e.account_manager, e.bq_post, e.total_post, e.total_events, e.unique_sku, e.makeup_events, e.skincare_events, e.fragrances_events, e.haircare_events, e.bath_events, e.fashion_events, e.eyewear_events, e.fashiontech_events, e.watches_events, e.beauty_events, e.snapchat_events, e.instagram_events, e.others_events, e.instastories_events, e.igtv_events, e.youtube_events from (SELECT * 
FROM aoi_agg WHERE order_date >= @min_agg_date
) a
LEFT JOIN (SELECT * 
FROM events_agg WHERE event_date >= @min_agg_date
) e 
ON e.event_date = a.order_date 
AND a.celebrity_id = e.celebrity_id 
UNION ALL
select IFNULL(a.order_date,event_date) activity_date, IFNULL(a.celebrity_id,e.celebrity_id) activity_celeb_id, IFNULL(a.celebrity_name,e.celebrity_name) activity_celebrity_name, a.*, event_date, e.celebrity_id evt_celebrity_id, e.account_manager, e.bq_post, e.total_post, e.total_events, e.unique_sku, e.makeup_events, e.skincare_events, e.fragrances_events, e.haircare_events, e.bath_events, e.fashion_events, e.eyewear_events, e.fashiontech_events, e.watches_events, e.beauty_events, e.snapchat_events, e.instagram_events, e.others_events, e.instastories_events, e.igtv_events, e.youtube_events from 
(SELECT * 
FROM events_agg WHERE event_date >= @min_agg_date
) e 
LEFT JOIN 
(SELECT * 
FROM aoi_agg WHERE order_date >= @min_agg_date
) a
ON e.event_date = a.order_date 
AND a.celebrity_id = e.celebrity_id 
WHERE a.celebrity_id IS NULL) a

LEFT JOIN (SELECT * 
FROM traffic_agg WHERE traffic_date >= @min_agg_date
) t 
ON (a.activity_date = t.traffic_date AND a.activity_celeb_id = t.celebrity_id)
group by a.activity_date, a.activity_celeb_id;

update celeb_agg
left join (select * from CELEBRITY_GROUP where GROUPID <> 13) cmap on 
cmap.CELEBID = celeb_agg.celebrity_id left join CELEB_GROUPS cord on cord.ID = cmap.GROUPID
SET celeb_agg.account_manager=cord.NAME_E Where celeb_agg.account_manager IS NULL;

update celeb_agg SET account_manager=celebrity_name Where account_manager IS NULL AND celebrity_id in(-9,-1,1);

