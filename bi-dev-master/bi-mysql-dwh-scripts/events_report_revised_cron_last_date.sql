select max(events_header_id) into @last_header_id from events_report; 
insert into events_report(event_id, events_header_id, user_name, celebrity_name, celebrity_id, generic,event_portal,
	event_type, event_class, bq_post, total_post, event_date, event_time, created_at,
	remark, labelid, productid, account_manager, category1, category2, category3, category4, brand, purchase_type) select event.* , cord.NAME_E 
account_manager, sku_details.cat1, sku_details.cat2, sku_details.cat3, sku_details.cat4, sku_details.brand, sku_details.purchase_type from (select 
concat(h.celebrity_id, '---', event_portal, '---',event_type, '---',event_date, '---',event_time)  event_id, h.id, h.user_name, h.celebrity_name, h.celebrity_id, generic, 
event_portal, event_type, event_class, bq_post, total_post, event_date, event_time, h.created_at, remark , IF(d.labelid IS NULL AND 
l.label IS NOT NULL AND d.skuid=l.sku,l.label,d.labelid) labelid, IF(d.skuid IS NULL AND l.sku IS NOT NULL AND 
d.labelid=l.label,l.sku,d.skuid) skuid from (select * from events.events_header where id > @last_header_id 
AND generic!='Generic') h join events.events_label_details d on h.id = d.event_id left join events.magento_celeb_prod l on  l.celebrity_id 
= h.celebrity_id and (d.labelid=l.label OR d.skuid=l.sku) group by event_id, skuid, labelid) event left join (select * from 
CELEBRITY_GROUP where GROUPID <> 13) cmap on cmap.CELEBID = event.celebrity_id left join CELEB_GROUPS cord on cord.ID = cmap.GROUPID left 
join (select i.PRODN skuid, c1.NAME_E cat1, c2.NAME_E cat2, c3.NAME_E cat3, c4.NAME_E cat4, brn.NAME_E brand, ptm.purchase_type from ITEMS i
	left join PRODUCT_CAT1 c1 on i.CAT1=c1.ID
	left join PRODUCT_CAT2 c2 on i.CAT2=c2.ID
	left join PRODUCT_CAT3 c3 on i.CAT3=c3.ID
	left join PRODUCT_CAT4 c4 FORCE INDEX(PRIMARY) on i.CAT4=c4.ID
	left join MK_BRANDS brn on i.BRAND_ID=brn.ID
	left join brand_purchase_type_master ptm on ptm.brand_name=brn.NAME_E
	group by i.PRODN) sku_details on sku_details.skuid = event.skuid; 

insert into events_report(event_id, events_header_id, user_name, celebrity_name, 
celebrity_id, generic,event_portal,
	event_type, event_class, bq_post, total_post, event_date, event_time, created_at,
	remark, labelid, productid, account_manager, category1, category2, category3, category4, brand, purchase_type) select event.* , cord.NAME_E 
account_manager, sku_details.cat1, sku_details.cat2, sku_details.cat3, sku_details.cat4, sku_details.brand, sku_details.purchase_type from (select 
concat(h.celebrity_id, '---', event_portal, '---',event_type, '---',event_date, '---',event_time) event_id, h.id, h.user_name, h.celebrity_name, h.celebrity_id, generic, 
event_portal, event_type, event_class, bq_post, total_post, event_date, event_time, h.created_at, remark , IF(d.labelid IS NULL AND 
l.label IS NOT NULL AND d.skuid=l.sku,l.label,d.labelid) labelid, IF(d.skuid IS NULL AND l.sku IS NOT NULL AND 
d.labelid=l.label,l.sku,d.skuid) skuid from (select * from events.events_header where id > @last_header_id 
AND generic='Generic') h join events.events_label_details d on h.id = d.event_id AND d.skuid !='All' AND d.labelid != 'All' left join 
events.magento_celeb_prod l on l.celebrity_id = h.celebrity_id and (d.labelid=l.label OR d.skuid=l.sku) group by event_id, skuid, 
labelid) event left join (select * from CELEBRITY_GROUP where GROUPID <> 13) cmap on cmap.CELEBID = event.celebrity_id left join 
CELEB_GROUPS cord on cord.ID = cmap.GROUPID left join (select i.PRODN skuid, c1.NAME_E cat1, c2.NAME_E cat2, c3.NAME_E cat3, c4.NAME_E 
cat4, brn.NAME_E brand, ptm.purchase_type from ITEMS i
	left join PRODUCT_CAT1 c1 on i.CAT1=c1.ID
	left join PRODUCT_CAT2 c2 on i.CAT2=c2.ID
	left join PRODUCT_CAT3 c3 on i.CAT3=c3.ID
	left join PRODUCT_CAT4 c4 FORCE INDEX(PRIMARY) on i.CAT4=c4.ID
	left join MK_BRANDS brn on i.BRAND_ID=brn.ID
	left join brand_purchase_type_master ptm on ptm.brand_name=brn.NAME_E
	group by i.PRODN) sku_details on sku_details.skuid = event.skuid;
	
	
insert into events_report(event_id, events_header_id, user_name, celebrity_name, celebrity_id, generic,event_portal,
	event_type, event_class, bq_post, total_post, event_date, event_time, created_at, remark, labelid, productid,
	account_manager, category1, category2, category3, category4, brand, purchase_type) select event.* , cord.NAME_E account_manager, 
sku_details.cat1, sku_details.cat2, sku_details.cat3, sku_details.cat4, sku_details.brand, sku_details.purchase_type from (select concat(h.celebrity_id, '---', event_portal, '---',event_type, '---',event_date, '---',event_time) event_id, h.id, h.user_name, h.celebrity_name, h.celebrity_id, generic, event_portal, event_type, event_class, bq_post, 
total_post, event_date, event_time, h.created_at, remark , l.label labelid, l.sku skuid from (select h.* from (select * from 
events.events_header where id > @last_header_id AND generic='Generic') h join events.events_label_details 
d on h.id = d.event_id AND (d.labelid='All' OR d.skuid='All') group by h.id)h left join events.magento_celeb_prod l on l.celebrity_id = 
h.celebrity_id group by event_id, skuid, labelid) event left join (select * from CELEBRITY_GROUP where GROUPID <> 13) cmap on 
cmap.CELEBID = event.celebrity_id left join CELEB_GROUPS cord on cord.ID = cmap.GROUPID left join (select i.PRODN skuid, c1.NAME_E cat1, 
c2.NAME_E cat2, c3.NAME_E cat3, c4.NAME_E cat4, brn.NAME_E brand, ptm.purchase_type from ITEMS i
	left join PRODUCT_CAT1 c1 on i.CAT1=c1.ID
	left join PRODUCT_CAT2 c2 on i.CAT2=c2.ID
	left join PRODUCT_CAT3 c3 on i.CAT3=c3.ID
	left join PRODUCT_CAT4 c4 FORCE INDEX(PRIMARY) on i.CAT4=c4.ID
	left join MK_BRANDS brn on i.BRAND_ID=brn.ID
	left join brand_purchase_type_master ptm on ptm.brand_name=brn.NAME_E
	group by i.PRODN) sku_details on sku_details.skuid = event.skuid;

