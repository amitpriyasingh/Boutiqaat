BEGIN;
DROP TABLE IF EXISTS aoi.nav_item_sku_master;

select * into aoi.nav_item_sku_master
FROM
(
  select
    ni.no as sku,
    ni.description as sku_name,
    ni.item_category_code as category_1,
    ni.product_group_code as category_2,
    ni.third_category as category_3,
    ni.fourth_category as category_4,
    ni.arabic_name as ar_name,
    ni.gender as gender,
    ni.color as color,
    ni.brand as brand,
    ni.size as size,
    CASE 
      WHEN ivd.cost is not null and ni.unit_cost is null THEN ivd.cost
      WHEN ivd.cost is not null and ni.unit_cost=ivd.cost THEN ivd.cost
      ELSE ni.unit_cost
    END as unit_cost
  FROM nav.item ni
  LEFT JOIN
  (
      SELECT 
          cost, 
          blocked, 
          vendor_no, 
          item_no 
      FROM
      (
          SELECT 
              row_number() OVER(partition by item_no,vendor_no order by start_date desc) rank,
              cost,
              blocked,
              vendor_no,
              item_no
          FROM nav.item_vendor_discount
      )
      WHERE blocked=0 and rank=1
  ) as ivd ON ivd.item_no = ni.no
);

COMMIT;