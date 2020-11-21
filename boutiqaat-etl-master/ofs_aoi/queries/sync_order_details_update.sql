#sync_order_details_update.sql 
START TRANSACTION;
update aoi.order_details_POC d 
    LEFT JOIN (select item_status_summary.item_id ItemId, cancelled_at, confirmed_at, readytoship_at, picked_at, order_allocated_at, packed_at, shipped_at, delivered_at, returned_at, recent_status.StatusId order_status_id, sm.StatusName last_status_name, recent_status.InsertedOn status_at
FROM (select (ItemId * 1) item_id, 
MIN(IF(StatusId IN (11), InsertedOn,NULL)) cancelled_at,
MIN(IF(StatusId IN (28), InsertedOn,NULL)) confirmed_at, 
MIN(IF(StatusId IN (6), InsertedOn,NULL)) readytoship_at,
MIN(IF(StatusId IN (4), InsertedOn,NULL)) picked_at,
MIN(IF(StatusId IN (5), InsertedOn,NULL)) order_allocated_at,
MIN(IF(StatusId IN (31,6), InsertedOn,NULL)) packed_at,
MIN(IF(StatusId IN (7), InsertedOn,NULL)) shipped_at,
MIN(IF(StatusId IN (8), InsertedOn,NULL)) delivered_at,
MIN(IF(StatusId IN (20,36,22,14,23,25,13), InsertedOn,NULL)) returned_at,
MAX(Id) last_entry_id, MAX(InsertedOn) last_status_at
FROM OFS.OrderStatus GROUP BY ItemId) item_status_summary
LEFT JOIN OFS.OrderStatus recent_status ON recent_status.Id = item_status_summary.last_entry_id
LEFT JOIN OFS.StatusMaster sm ON recent_status.StatusId = sm.ID) irs ON irs.ItemId = d.item_id 
LEFT JOIN OFS.OrderBatchDetails batch ON batch.ItemId = d.item_id 
LEFT JOIN OFS.BoxLine bl on bl.ItemId=d.item_id
SET d.batch_id = batch.BatchId,
    d.batch_inserted_at = (batch.InsertedOn+INTERVAL 3 HOUR),
    d.awbno = bl.AWBNo, 
    d.order_status_id = irs.order_status_id,
    d.order_status = irs.last_status_name,
    d.last_activity = irs.last_status_name,
    d.status_at = CONVERT_TZ(irs.status_at,'+00:00','+03:00'), d.cancelled_at=CONVERT_TZ(irs.cancelled_at,'+00:00','+03:00'), d.confirmed_at=CONVERT_TZ(irs.confirmed_at,'+00:00','+03:00'), d.readytoship_at=CONVERT_TZ(irs.readytoship_at,'+00:00','+03:00'),
    d.picked_at=CONVERT_TZ(irs.picked_at,'+00:00','+03:00'), d.order_allocated_at=CONVERT_TZ(irs.order_allocated_at,'+00:00','+03:00'), d.packed_at=CONVERT_TZ(irs.packed_at,'+00:00','+03:00'), d.shipped_at=CONVERT_TZ(irs.shipped_at,'+00:00','+03:00'), d.delivered_at=CONVERT_TZ(irs.delivered_at,'+00:00','+03:00'), d.returned_at=CONVERT_TZ(irs.returned_at,'+00:00','+03:00');

update aoi.order_details_POC d JOIN (select ItemId, InsertedOn from (select ItemId, COALESCE(UpdatedOn,InsertedOn) InsertedOn, Deleted from (select * from OFS.HoldOrders order by Id desc) ordered_ho GROUP BY ItemId) ho WHERE Deleted=0)held_items ON held_items.ItemId=d.item_id
SET d.order_status_id = 10, d.order_status = 'Hold', d.status_at = CONVERT_TZ(held_items.InsertedOn,'+00:00','+03:00');

update aoi.order_details_POC d JOIN OFS.CancelledOrders cancelled_items ON cancelled_items.ItemId=d.item_id
SET d.order_status_id = 11, d.order_status = 'Cancel', d.status_at = CONVERT_TZ(cancelled_items.InsertedOn,'+00:00','+03:00'), d.cancelled_at= CONVERT_TZ(cancelled_items.InsertedOn,'+00:00','+03:00');

update aoi.order_details_POC d 
SET d.status_grouped_ops = 
    CASE WHEN d.order_status IN ('CRM ASSIGNED','CRM PENDING','Order Received','RECEIVED','UnHold') THEN '01. Pending Confirmation' 
    WHEN d.order_status IN ('Hold') THEN '02. Hold' 
    WHEN d.order_status IN ('CONFIRM','ReadyForWhs') THEN '03. Pending batch creation' 
    WHEN d.order_status IN ('BatchCreated','ReadyForPicking') THEN '04. WIP - Picking' 
    WHEN d.order_status IN ('PNA') THEN '05. PNA' 
    WHEN d.order_status IN ('Picked') THEN '06. WIP - Sorting' 
    WHEN d.order_status IN ('OrderAllocation') THEN '07. WIP - Invoicing & Packing' 
    WHEN d.order_status IN ('PACKED','ReadytoShip','WAREHOUSE PROCESSED') THEN '08. Ready to ship' 
    WHEN d.order_status IN ('Packet With Driver','Shipped') THEN '09. Shipped' 
    WHEN d.order_status IN ('CLOSE','Delivered','PAID') THEN '10. Delivered' 
    WHEN d.order_status IN ('Lost By Driver') THEN '09. Lost' 
    WHEN d.order_status IN ('Customer Denied') THEN '12. Return - NDR' 
    WHEN d.order_status IN ('Cancel') THEN '11. Cancelled' 
    WHEN d.order_status IN ('PendingforRefund','RetrunQCPass','RetunPutaway','ReturnClose','ReturnGateEntry','ReturnInitiated','ReturnPass/Fail', 'ReturnQC', 'ReturnQCFail', 'ReturnRecieved') THEN '12. Return / Refund' 
    ELSE CONCAT('13. ',d.order_status) END,
 d.status_grouped_mgmt = CASE WHEN d.order_status IN ('Order Received','UnHold') THEN '1. Not Confirmed' 
 WHEN d.order_status IN ('Hold') THEN '2. Hold' 
 WHEN d.order_status IN ('CONFIRM','ReadyForWhs','BatchCreated','Picked','OrderAllocation','ReadytoShip','ReadyForPicking','PNA','PACKED','WAREHOUSE PROCESSED') THEN '4. WIP WH' 
 WHEN d.order_status IN ('Shipped','Packet With Driver') THEN '5. Shipped' 
 WHEN d.order_status IN ('CLOSE','Delivered','PAID') THEN '6. Delivered' 
 WHEN d.order_status IN ('Cancel') THEN '3. Cancelled' 
 WHEN d.order_status IN ('PendingforRefund','RetrunQCPass','RetunPutaway','ReturnClose','ReturnGateEntry','ReturnInitiated','ReturnPass/Fail', 'ReturnQC', 'ReturnQCFail', 'ReturnRecieved') THEN '7. Returned' 
 WHEN d.order_status IN ('Lost By Driver') THEN '5. Lost'
 WHEN d.order_status IN ('Customer Denied') THEN '7. Return NDR'
 ELSE CONCAT('8. ',d.order_status) END;

update order_details left join soh_report on soh_report.sku=order_details.sku set order_details.brand=soh_report.brand where soh_report.brand is not null and order_details.order_date >= DATE(now())-INTERVAL 3 DAY;
update order_details left join soh_report on soh_report.sku=order_details.sku set order_details.sku_name=soh_report.sku_name where soh_report.sku_name is not null and order_details.order_date >= DATE(now())-INTERVAL 3 DAY;
update order_details left join soh_report on soh_report.sku=order_details.sku set order_details.category1=soh_report.category1 where soh_report.category1 is not null and order_details.order_date >= DATE(now())-INTERVAL 3 DAY;
update order_details left join soh_report on soh_report.sku=order_details.sku set order_details.category2=soh_report.category2 where soh_report.category2 is not null and order_details.order_date >= DATE(now())-INTERVAL 3 DAY;

DELETE from aoi.order_details_POC where item_id=0;
COMMIT;


update aoi.order_details_POC odd left join 
(select cp1.sku sku,(case when cpt.value='4194' then 'Female' 
when cpt.value='2741' then 'Male' 
when cpt.value='4194,2741' or cpt.value='2741,4194' then 'Both' 
else 'Both' end) gender from events.catalog_product_entity cp1 
left join (select * from magento_bi.catalog_product_entity_text where attribute_id=542) cpt 
on cpt.row_id=cp1.row_id group by 1) sgd on odd.sku=sgd.sku 
set odd.gender = sgd.gender where odd.order_date > subdate(current_date, interval 5 day);

update aoi.order_details_POC od left join OFS.BoxStatus bs on od.awbno = bs.AWBNo set od.is_ndr = 1 , od.delivered_at = bs.DeliveredDateTime where od.delivered_at is NULL and od.returned_at is not NULL;