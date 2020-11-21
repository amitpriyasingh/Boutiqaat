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
  PRIMARY KEY (id)
);
INSERT INTO customer_first_order (customer_id, customer_email, customer_group, telephone_no, first_order_date, entity_id, increment_id, tranid, reference_number)
SELECT s.customer_id, s.customer_email, s.customer_group, s.telephone_no, s.created_at first_order_date, s.entity_id, s.increment_id, s.tranid, s.reference_number FROM sales_order_grid s
JOIN (SELECT min(created_at) fod, customer_id cid FROM sales_order_grid WHERE customer_id is not NULL group by cid) m 
ON s.created_at = m.fod AND s.customer_id = m.cid GROUP BY s.customer_id;
delete from customer_first_order where customer_email in ('developer@boutiqaat.com', 'tester@boutiqaat.com', 'businesstest@boutiqaat.com') or customer_email like '%abc%' or customer_email like '%test%' or customer_email like  '%load%';