INSERT INTO c_activity (customer_id, telephone_no, first_order_date, order_number, country, device_type, payment_method, city, order_region)
SELECT m.customer_id, m.customer_telephone, date(m.order_date) first_order_date, m.order_number,
m.country, m.device_type, m.payment_method, m.city, m.order_region
FROM (select * from aoi where customer_telephone not in (select telephone_no from c_activity) and country <> 'Celebrities') m
JOIN (SELECT min(date(order_date)) fod, customer_telephone cid FROM aoi WHERE customer_telephone is not NULL group by cid) min 
ON date(m.order_date) = min.fod AND m.customer_telephone = min.cid GROUP BY m.customer_telephone ;
update aoi a join c_activity c on a.order_number = c.order_number set a.is_first_order=1 where c.order_number is not NULL and  a.order_date > subdate(current_date, interval 15 day);
