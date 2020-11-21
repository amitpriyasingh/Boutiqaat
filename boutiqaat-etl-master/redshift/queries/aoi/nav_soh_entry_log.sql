BEGIN;
DROP TABLE IF EXISTS aoi.soh_entry_log;

SELECT * INTO aoi.soh_entry_log
FROM 
(select 
    final.vendor_name as supplier_name,
    final.vendor_item_no as supplier_item_number,
    final.vendor_no as vendor_code,
    final.attribute_desc as brand_name,
    final.item_no  as sku, 
    final.sku_2 as sku_2, 
    final.barcode as barcode,
    final.item_category_code as category_1, 
    final.product_group_code as category_2,
    final.third_category as category_3,
    final.fourth_category as category_4,
    final.item_description as item_description,
    sales_price.unit_price as retail_price,
    item_ven_disc.cost as cost_price,
    final.tot_quantity as soh,
    ILE.first_grn_date as first_grn_date,
    ILE.last_grn_date as last_grn_date,
    final.payment_terms_code as payment_terms,
    final.country_region_code as country, 
    final.location as store_wh_location,
    GETDATE() as updated_at_utc
from (  Select
            tbl1.location as location,
            tbl1.item_no as item_no,
            tbl1.sku_2 as sku_2,
            tbl1.barcode as barcode,
            tbl1.brand as brand,
            tbl1.product_group_code as product_group_code, 
            tbl1.item_category_code as item_category_code,
            tbl1.third_category as third_category,
            tbl1.fourth_category as fourth_category, 
            tbl1.item_description as item_description,
            tbl2.tot_quantity as tot_quantity,
            VenDetails.vendor_name as vendor_name,
            VenDetails.vendor_item_no as vendor_item_no,
            VenDetails.vendor_no as vendor_no,
            VenDetails.payment_terms_code as payment_terms_code,
            VenDetails.country_region_code as country_region_code,
            attribute_master.attribute_desc as attribute_desc
    /* Take All Location With All Items Combination
    */
        from 
        (   select 
                wh_loc.location as location,
                item_master.item_no as item_no , 
                item_master.sku_2 as sku_2, 
                item_master.barcode as barcode,
                item_master.brand as brand,
                item_master.product_group_code as product_group_code, 
                item_master.item_category_code as item_category_code,
                item_master.third_category as third_category,
                item_master.fourth_category as fourth_category, 
                item_master.item_description as item_description
            from 
            (   select 
                    code as location
                from nav.location
                WHERE bin_mandatory=1 
                AND code IN (SELECT  location_code from nav.warehouse_entry)
            ) as wh_loc
		    Cross Join 
            (   select 
                    no as item_no, 
                    no_2 as sku_2, 
                    ean_code as barcode, 
                    brand, 
                    product_group_code, 
                    item_category_code, 
                    third_category, 
                    fourth_category, 
                    description as item_description 
                from nav.item 
            ) as item_master
        ) as tbl1
        /*  Take Data from Whare house Entry and map to Above Tb1*/
        left join
        (   select
                Sum( COALESCE(quantity,0) ) tot_quantity,
                location_code,
                item_no as item_no
            from nav.warehouse_entry
            where bin_type_code<>'SHIP'
            Group by  location_code,item_no  ) as tbl2
        on  tbl2.location_code=tbl1.location and tbl2.item_no = tbl1.item_no
        /* Take Info and Map with Vendor and Vendor Items */
        left  Join
        (   select 
                name as vendor_name,
                iv.vendor_item_no vendor_item_no,
                payment_terms_code,
                country_region_code,
                iv.item_no,
                iv.vendor_no
            from  nav.vendor as v
            left Join 
            (   select 
                    item_no,vendor_item_no, vendor_no 
                from 
                (   select 
                        row_number() OVER (partition by item_no order by ts desc) rank,
                        item_no,
                        vendor_item_no,
                        vendor_no 
                    from nav.item_vendor
                ) 
                where  rank = 1
            ) as iv
            on v.no=iv.vendor_no
        ) as VenDetails on VenDetails.item_no=tbl1.item_no
        left join
        (   select  
                description as attribute_desc,
                code  
            from  nav.item_attribute_master
            where attribute_type=1
        ) as attribute_master 
        on attribute_master.code=tbl1.brand
    ) as final
    left Join (Select  
                    min(posting_date) first_grn_date, 
                    max(posting_date) last_grn_date ,
                    item_no as lg_item_no 
                    from nav.item_ledger_entry
                    WHERE entry_type=0 
                    and   document_type = 5 
                    GROUP BY item_no
    )as ILE on ILE.lg_item_no= final.item_no
    left join (select cost, blocked, vendor_no, item_no from
        (select row_number() OVER(partition by item_no,vendor_no order by start_date desc) rank,
        cost,
        blocked,
        vendor_no,
        item_no
        from nav.item_vendor_discount)
        where blocked=0 and rank=1) item_ven_disc
    ON item_ven_disc.item_no=final.item_no 
    AND item_ven_disc.vendor_no = final.vendor_no
    left join(select
    	item_no,
        unit_price
        from  nav.sales_price
        where ending_date = '1753-01-01 00:00:00.000') as sales_price
    ON sales_price.item_no=final.item_no);

COMMIT;