update events_report er
left join (select a.sku sku, a.celebrity_id, a.celebrity_name, b.PRODNAME_E name, a.label from events.magento_celeb_prod a left join data_lake.ITEMS b on a.sku=b.PRODN) s on er.productid=s.sku and s.celebrity_id = er.celebrity_id and s. celebrity_name = er.celebrity_name
left join (select a.sku sku, a.celebrity_id, a.celebrity_name, b.PRODNAME_E name, a.label from events.magento_celeb_prod a left join data_lake.ITEMS b on a.sku=b.PRODN) l on er.labelid=l.label and l.celebrity_id = er.celebrity_id and l.celebrity_name = er.celebrity_name 
left join (select count(*) line, event_id from events_report group by event_id) abq on er.event_id = abq.event_id
set er.sku_name = coalesce(s.name, l.name),
er.sku_code = if(er.event_type = "Generic", NULL, er.productid),
er.sku_code_name = CONCAT(if(er.event_type = "Generic", '----', er.productid), " : ", coalesce(s.name, l.name)),
er.alloc_bq_post = (er.bq_post/abq.line);
