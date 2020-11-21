select * from (
    select 
        ROW_NUMBER() OVER(ORDER BY ingestion_date) as rownum,
        * 
    from adjust.raw_data where ingestion_date = '{{DATE}}') as sub 
WHERE \$CONDITIONS