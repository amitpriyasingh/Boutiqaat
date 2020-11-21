update aoi.bi_celebrity_master set code=NULL;
update aoi.order_details i left join aoi.bi_celebrity_master bicm on bicm.celebrity_id=i.celebrity_id set i.celebrity_code=bicm.code;

