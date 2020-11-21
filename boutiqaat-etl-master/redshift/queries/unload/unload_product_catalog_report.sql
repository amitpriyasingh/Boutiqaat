UNLOAD ('select * from analytics.product_catalog_report') 
to 's3://btq-etl/redshift/product_catalog_report/product_catalog_report_'
iam_role 'arn:aws:iam::652586300051:role/redshift-s3-writeAccess'
parallel off
ALLOWOVERWRITE
HEADER           
DELIMITER '\t'
