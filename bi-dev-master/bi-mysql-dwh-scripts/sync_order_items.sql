START TRANSACTION;

select max(item_id) into @max_item_id from aoi.order_items;
delete from aoi.order_items where order_number in (select order_number from aoi.order_details where item_id=@max_item_id);
select max(item_id) into @max_item_id from aoi.order_items;

INSERT INTO aoi.order_items (order_item_index, order_number          ,phone_no, item_id               ,batch_id              ,awbno                 ,sku                   ,sku_name              ,category1             ,category2             ,brand                 ,celebrity_code        ,celebrity_id          ,celebrity_name        ,account_manager       ,quantity              ,order_currency        ,exchange_rate         ,net_sale_price        ,rrp                   ,list_price            ,shipping_charge       ,cod_charge            ,allocated_order_count ,order_date            ,order_at              ,order_date_utc        ,order_at_utc          ,order_type            ,order_category        ,payment_method        ,payment_gateway       ,customer_id           ,billing_phone_no      ,billing_country       ,shipping_phone_no     ,shipping_country      ,net_sale_price_kwd    ,shipping_charge_kwd   ,cod_charge_kwd   , order_status_id        ,order_status           ,status_grouped_mgmt   ,status_grouped_ops    ,last_activity         ,status_at             ,cancelled_at          ,confirmed_at          ,readytoship_at        ,picked_at             ,order_allocated_at    ,packed_at             ,shipped_at            ,delivered_at          ,returned_at           ,batch_inserted_at     ,order_inserted_on_utc)
SELECT order_item_index, order_number          ,COALESCE(billing_phone_no,shipping_phone_no), item_id               ,batch_id              ,awbno                 ,sku                   ,sku_name              ,category1             ,category2             ,brand                 ,celebrity_code        ,celebrity_id          ,celebrity_name        ,account_manager       ,quantity              ,order_currency        ,exchange_rate         ,net_sale_price        ,rrp                   ,list_price            ,shipping_charge       ,cod_charge          ,allocated_order_count ,order_date            ,order_at              ,order_date_utc        ,order_at_utc          ,order_type            ,order_category        ,payment_method        ,payment_gateway       ,customer_id           ,billing_phone_no      ,billing_country       ,shipping_phone_no     ,shipping_country      ,net_sale_price_kwd    ,shipping_charge_kwd   ,cod_charge_kwd   , order_status_id        ,order_status           ,status_grouped_mgmt   ,status_grouped_ops    ,last_activity         ,status_at             ,cancelled_at          ,confirmed_at          ,readytoship_at        ,picked_at             ,order_allocated_at    ,packed_at             ,shipped_at            ,delivered_at          ,returned_at           ,batch_inserted_at     ,order_inserted_on_utc FROM
(SELECT CONCAT(order_number,'-',@order_item_index:=CASE WHEN @order_number=order_number THEN @order_item_index+1 ELSE 1 END) AS order_item_index,@order_number:=order_number AS order_num, t1.* from (select * from aoi.order_details where item_id > @max_item_id) t1, (SELECT @order_item_index:=0,@order_number:='') t2
ORDER BY order_number) order_items;

update aoi.order_items c inner join (select * from aoi.order_details where item_id <= @max_item_id) d on c.item_id=d.item_id
SET c.status_grouped_ops = d.status_grouped_ops, c.status_grouped_mgmt = d.status_grouped_mgmt,
c.order_status_id=d.order_status_id, c.order_status=d.order_status, c.status_at=d.status_at, c.cancelled_at=d.cancelled_at,
c.batch_id = d.batch_id,c.batch_inserted_at=d.batch_inserted_at,
c.awbno=d.awbno, c.last_activity=d.last_activity,c.status_at=d.status_at, c.cancelled_at=d.cancelled_at, c.confirmed_at=d.confirmed_at, c.readytoship_at=d.readytoship_at,
c.picked_at=d.picked_at, c.order_allocated_at=d.order_allocated_at, c.packed_at=d.packed_at, c.shipped_at=d.shipped_at, c.delivered_at=d.delivered_at, c.returned_at=d.returned_at,
c.celebrity_name = d.celebrity_name, c.celebrity_code = d.celebrity_code, c.account_manager = d.account_manager;

UPDATE aoi.order_items t5 INNER JOIN
(select order_number, 
CASE WHEN TRIM(phone_no)<>'' THEN customer_ltoc ELSE -1 END customer_ltoc from 
	(select t1.order_number, 
	@customer_ltoc:=(CASE WHEN @phone_no=phone_no THEN @customer_ltoc+1 ELSE 1 END) as customer_ltoc, @phone_no:=phone_no as phone_no from 
		(select COALESCE(order_at,order_date) order_at, order_number, COALESCE(billing_phone_no,shipping_phone_no) phone_no 
                from aoi.order_items 
		where customer_ltoc=0 order by order_at, order_number) as t1, 
		(select @customer_ltoc:=0,@phone_no:='') as t2 
	order by t1.phone_no) as t3
) as t4 ON t5.order_number=t4.order_number SET t5.customer_ltoc=t4.customer_ltoc, t5.is_first_order=(CASE WHEN t4.customer_ltoc=1 THEN 1 ELSE 0 END);

UPDATE aoi.order_items all_items inner join (select phone_no, MIN(CAST(order_number AS UNSIGNED)) first_ord_no from aoi.order_items group by phone_no) first_orders on all_items.order_number=first_orders.first_ord_no SET all_items.is_first_order=1;

COMMIT;

