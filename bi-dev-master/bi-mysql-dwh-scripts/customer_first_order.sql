drop table if exists sales_order_grid_filtered;
create table sales_order_grid_filtered as 
  select  * from sales_order_grid 
  where (payment_method = 'knet_cc' and result ='CAPTURED') 
  or (payment_method= 'md_cybersourcesa' and result ='ACCEPT')  
  or (payment_method = 'knet'    and result ='CAPTURED' )  
  or (payment_method = 'msp_cashondelivery' ) 
  or (payment_method = 'cashondelivery'  ) ;
drop table if exists customer_first_order;
CREATE TABLE `customer_first_order` (
  `id` int(10) AUTO_INCREMENT,
  `customer_id` int(10) unsigned UNIQUE DEFAULT NULL COMMENT 'Customer Id',
  `customer_email` varchar(255) DEFAULT NULL,
  `customer_group` varchar(255) DEFAULT NULL,
  `telephone_no` varchar(255) DEFAULT NULL,
  `first_order_date` timestamp NULL DEFAULT NULL COMMENT 'Created At',
  `entity_id` int(10) unsigned NOT NULL COMMENT 'Entity Id',
  `increment_id` varchar(50) DEFAULT NULL COMMENT 'Increment Id',
  `tranid` varchar(255) DEFAULT NULL COMMENT 'Transaction Id of order',
  `reference_number` varchar(255) DEFAULT NULL COMMENT 'Reference number of order from middle layer',
  `country` varchar(255) DEFAULT NULL COMMENT 'From first digit of increment_id',
  PRIMARY KEY (id)
);
INSERT INTO customer_first_order (customer_id, customer_email, customer_group, telephone_no, first_order_date, entity_id, increment_id, tranid, reference_number, country)
SELECT s.customer_id, s.customer_email, s.customer_group, s.telephone_no, s.created_at first_order_date, s.entity_id, s.increment_id, s.tranid, s.reference_number, (case when CHAR_LENGTH(s.increment_id) = 9 AND SUBSTR(s.increment_id,1,1) in ('1','3') THEN 'Kuwait'
  when CHAR_LENGTH(s.increment_id) = 9 AND SUBSTR(s.increment_id,1,1) in ('4','5') THEN 'Qatar'
  when CHAR_LENGTH(s.increment_id) = 9 AND SUBSTR(s.increment_id,1,1) in ('6','7') THEN 'UAE'
  when CHAR_LENGTH(s.increment_id) = 9 AND SUBSTR(s.increment_id,1,1) in ('8','9') THEN 'KSA'
  when CHAR_LENGTH(s.increment_id) = 10 AND SUBSTR(s.increment_id,1,2) in ('10','11') THEN 'Bahrain'
  when CHAR_LENGTH(s.increment_id) = 11 AND SUBSTR(s.increment_id,1,2) in ('12','13') THEN 'Oman'
  else 'Unspecified'
end )
FROM sales_order_grid_filtered s
JOIN (SELECT min(created_at) fod, customer_id cid FROM sales_order_grid_filtered WHERE customer_id is not NULL group by cid) m 
ON s.created_at = m.fod AND s.customer_id = m.cid GROUP BY s.customer_id;
delete from customer_first_order where customer_email in ('developer@boutiqaat.com', 'tester@boutiqaat.com', 'businesstest@boutiqaat.com') or customer_email like '%abc%' or customer_email like '%test%' or customer_email like  '%load%';
