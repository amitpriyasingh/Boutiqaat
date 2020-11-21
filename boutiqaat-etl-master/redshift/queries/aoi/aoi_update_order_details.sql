DROP TABLE IF EXISTS tmp_orders_celeb;
create TEMPORARY TABLE tmp_orders_celeb
as
(
    select 
        items.item_id, 
        items.celebrity_id, 
        celeb_master.account_manager, 
        celeb_master.celebrity_code, 
        celeb_master.celebrity_name 
    from 
    (
        select * 
        from aoi.sales_order_items 
    )items 
    left join
    (
        select 
            celebrity_id,  
            current_am account_manager, 
            am_email, 
            code celebrity_code, 
            celebrity_name,
            max(am_effective_date) am_start_date, 
            CASE 
            	WHEN max(am_mapping_end_date) IS NULL THEN '2100-01-01 00:00:00' 
            	ELSE MAX(am_mapping_end_date) END as am_end_date, 
            MAX(last_updated_utc) last_updated_utc 
        from 
        (
            select * 
            from aoi.bi_celebrity_master 
            order by last_updated_utc desc
        ) ordered_bcm 
        group by 1,2,3,4,5
    ) celeb_master
    on items.celebrity_id=celeb_master.celebrity_id 
    AND order_date between am_start_date AND am_end_date
);

BEGIN;
UPDATE aoi.sales_order_items 
SET celebrity_code=toc.celebrity_code,
celebrity_name=toc.celebrity_name,
account_manager=toc.account_manager
FROM aoi.sales_order_items od 
JOIN tmp_orders_celeb  toc 
ON od.celebrity_id=toc.celebrity_id AND od.item_id=toc.item_id
WHERE od.item_id is not null and od.celebrity_id is not null;
COMMIT;

create TEMPORARY TABLE tmp_orders_hold
as
(
    select 
        item_id, 
        COALESCE(MAX(updated_on), MAX(inserted_on)) inserted_on 
    from ofs.hold_orders 
    where deleted=0 
    group by 1
);

BEGIN;
UPDATE aoi.sales_order_items 
SET order_status_id = 10,
order_status = 'Hold',
status_updated_at = toh.inserted_on 
FROM aoi.sales_order_items od 
JOIN tmp_orders_hold  toh 
ON od.item_id=toh.item_id;
COMMIT;

BEGIN;
UPDATE aoi.sales_order_items 
SET order_status_id = 11,
order_status = 'Cancel',
status_updated_at = co.inserted_on 
FROM aoi.sales_order_items od 
JOIN ofs.cancelled_orders  co 
ON od.item_id=co.item_id;
COMMIT;