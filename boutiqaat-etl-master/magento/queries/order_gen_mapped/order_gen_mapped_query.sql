
SELECT
so.order_number,
cgf.gender,
cgf.email 
FROM 
(
    SELECT 
        cast(increment_id as UNSIGNED) as order_number, 
        customer_id
    FROM boutiqaat_v2.sales_order 
) so
JOIN 
    boutiqaat_v2.customer_grid_flat cgf 
ON cgf.entity_id=so.customer_id
WHERE \$CONDITIONS
group by 1
