call dow_aoi();
drop table if exists status_history;
CREATE TABLE `status_history` (
  `order_no` varchar(30) NOT NULL,
  `status_date` timestamp(6) NULL DEFAULT NULL,
  `status`  varchar(30) DEFAULT NULL,
  `delivery_date` datetime(6) DEFAULT NULL,
  `dispatch_date` datetime(6) DEFAULT NULL,
  PRIMARY KEY (`ORDER_NO`)
);
insert into status_history (order_no, status_date, delivery_date, dispatch_date, status)
  select a.*, (select status from MAGENTO_ORDSTATUS_HIST where order_no= a.ono and status_date=a.sd) from 
  (select ORDER_NO ono, max(status_date) sd, max(if(status in ('Deliverd', 'Delivered'),status_date,NULL)) delivery_date, max(if(status = 'Shipped', status_date,NULL)) dispatch_date from MAGENTO_ORDSTATUS_HIST group by ORDER_NO) a;
update aoi a 
  left join status_history sh on a.order_number = sh.order_no
  set a.status = sh.status,
  a.status_group = (CASE sh.status 
                  WHEN 'Canceled' THEN 'Cancelled'
                  WHEN 'Hold' THEN 'Hold'
                  WHEN 'WH-Hold' THEN 'Hold'
                  WHEN 'HoldConfirmed ' THEN 'Hold'
                  WHEN '%' THEN 'Cancelled'
                  WHEN 'Returned' THEN 'Returned'
                  WHEN 'Deliverd' THEN 'Success'
                  WHEN 'Paid' THEN 'Success'
                  WHEN 'Shipped' THEN 'Success'
                  WHEN 'Confirmed' THEN 'Unshipped'
                  WHEN 'Invoiced' THEN 'Unshipped'
                  WHEN 'PreConfirmed' THEN 'Unshipped'
                  WHEN 'Received' THEN 'Unshipped'
                  WHEN 'Shelved' THEN 'Unshipped'
                  WHEN 'Reschedule' THEN 'Unshipped'
                  WHEN 'ScheduleRq' THEN 'Cancelled'
                  WHEN 'Cancel Rq' THEN 'Cancelled'
                  WHEN 'CancelRq' THEN 'Cancelled'
                  ELSE 'Not Specified' END),
  a.status_date = sh.status_date,
  a.delivery_date = sh.delivery_date,
  a.dispatch_date = sh.dispatch_date,
  a.is_first_order = if(a.order_number in (select order_number from c_activity),1,0);
