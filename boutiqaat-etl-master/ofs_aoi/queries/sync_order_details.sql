#sync_order_details.sql 
START TRANSACTION;

select max(order_inserted_on_utc) into @max_order_inserted_on_utc from aoi.order_details_POC; select max(item_id) into @max_item_id from aoi.order_details_POC;

replace into aoi.order_details_POC(order_number, item_id, batch_id, batch_inserted_at, sku, sku_name, category1, category2, brand, celebrity_code, celebrity_id, celebrity_name, account_manager, quantity, order_currency, exchange_rate, net_sale_price, rrp, list_price, shipping_charge, cod_charge, allocated_order_count,order_date, order_at, order_date_utc, order_at_utc, order_type, order_category, payment_method, payment_gateway, customer_id, billing_phone_no, billing_country, shipping_phone_no, shipping_country, net_sale_price_kwd, shipping_charge_kwd, cod_charge_kwd, order_inserted_on_utc) select isl.WebOrderNo order_number, isl.ItemId item_id, batch.BatchId batch_id, (batch.InsertedOn+INTERVAL 3 HOUR), isl.ItemNo sku, scb.sku_name  sku_name, scb.category1, scb.category2, scb.brand, NULL, IF(ipl.AgentCode*1=0,NULL,ipl.AgentCode*1) celebrity_id, NULL, NULL, isl.Quantity quantity, ipl.CurrencyCode order_currency, (1.0/ipl.CurrencyFactor) exchange_rate, ipl.UnitPrice net_sale_price, ipl.MRPPrice rrp, (ipl.UnitPrice + ipl.CouponAmount) list_price, (order_shipping_charges * (ipl.UnitPrice / order_sum_unit_price)) shipping_charge, (order_cod_charges * (ipl.UnitPrice / order_sum_unit_price)) cod_charge, (1.0/item_count) allocated_order_count, date(CONVERT_TZ(ish.OrderDateTime,'+00:00','+03:00')) order_date, CONVERT_TZ(ish.OrderDateTime,'+00:00','+03:00') order_at, date(ish.OrderDateTime) order_date_utc, ish.OrderDateTime order_at_utc, cast(ish.OrderType as char) order_type, ish.OrderCategory order_category, 
order_payment_method, order_payment_gateway,
ish.CustomerId customer_id, ioa.billing_phone_no, ioa.billing_country, ioa.shipping_phone_no, ioa.shipping_country, (ipl.UnitPrice*(1.0/ipl.CurrencyFactor)) net_sale_price_kwd, (order_shipping_charges * (ipl.UnitPrice / order_sum_unit_price) *(1.0/ipl.CurrencyFactor)) shipping_charge_kwd, (order_cod_charges * (ipl.UnitPrice / order_sum_unit_price) *(1.0/ipl.CurrencyFactor)) cod_charge_kwd, 
ish.order_inserted_on_utc FROM

(select ish.WebOrderNo, ish.InsertedOn order_inserted_on_utc, ish.OrderDateTime, COALESCE(ish.PaymentMethodCode, ipl.PaymentMethodCode) order_payment_method, 
COALESCE(ish.PaymentGateway, ipl.PaymentGateway) order_payment_gateway, ish.OrderType, ish.OrderCategory, ish.CustomerId,  SUM(IF(ipl.IsHeader=1,ShippingCharges,0)) order_shipping_charges, SUM(IF(ipl.IsHeader=1,CODCharges,0)) order_cod_charges, SUM(IF(ipl.IsHeader=0,UnitPrice,0)) order_sum_unit_price, COUNT(DISTINCT ItemId) item_count FROM OFS.InboundSalesHeader ish 
LEFT JOIN OFS.InboundPaymentLine ipl ON ish.WebOrderNo = ipl.WebOrderNo
WHERE ish.InsertedOn >= @max_order_inserted_on_utc GROUP BY ish.WebOrderNo) ish
LEFT JOIN OFS.InboundSalesLine isl ON ish.WebOrderNo = isl.WebOrderNo
LEFT JOIN aoi.nav_item_sku_master scb ON scb.sku = isl.ItemNo
LEFT JOIN OFS.InboundPaymentLine ipl on ipl.IsHeader=0 and isl.ItemId = ipl.ItemId and ipl.WebOrderNo=isl.WebOrderNo 
LEFT JOIN OFS.OrderBatchDetails batch ON batch.ItemId = isl.ItemId
LEFT JOIN (SELECT WebOrderNo, CustomerId,
GROUP_CONCAT(CASE WHEN ioa.addressdetailtype='Bill' THEN COALESCE(ioa.PhoneNo,ioa.AlternatePhoneNo) END) billing_phone_no, 
GROUP_CONCAT(CASE WHEN ioa.addressdetailtype='Bill' THEN ioa.Country END) billing_country, 
GROUP_CONCAT(CASE WHEN ioa.addressdetailtype='Ship' THEN COALESCE(ioa.PhoneNo,ioa.AlternatePhoneNo) END) shipping_phone_no, 
GROUP_CONCAT(CASE WHEN ioa.addressdetailtype='Ship' THEN ioa.Country END) shipping_country
FROM OFS.InboundOrderAddress ioa where CustomerId!='null' GROUP BY WebOrderNo, CustomerId) ioa on isl.WebOrderNo = ioa.WebOrderNo
WHERE isl.WebOrderNo IS NOT NULL AND order_inserted_on_utc > @max_order_inserted_on_utc OR (order_inserted_on_utc = @max_order_inserted_on_utc AND isl.ItemId > @max_item_id );

update aoi.order_details_POC items join
(select items.item_id, items.celebrity_id, celeb_master.account_manager, celeb_master.celebrity_code, celeb_master.celebrity_name from (select * from aoi.order_details_POC WHERE order_inserted_on_utc > @max_order_inserted_on_utc OR (order_inserted_on_utc = @max_order_inserted_on_utc AND item_id > @max_item_id ))items left join
(select celebrity_id, code celebrity_code, celebrity_name, current_am account_manager, am_email, max(am_effective_date) am_start_date, IF(max(am_mapping_end_date) IS NULL, '2100-01-01',MAX(am_mapping_end_date)) am_end_date, MAX(last_updated_utc) last_updated_utc from (select * from aoi.bi_celebrity_master order by last_updated_utc desc) ordered_bcm group by celebrity_id, account_manager, am_email) celeb_master
on items.celebrity_id=celeb_master.celebrity_id where order_date between am_start_date AND am_end_date) corrected_items on corrected_items.item_id=items.item_id
SET items.celebrity_name = corrected_items.celebrity_name, items.celebrity_code = corrected_items.celebrity_code, items.account_manager = corrected_items.account_manager;

DELETE from aoi.order_details_POC where item_id=0;
COMMIT;