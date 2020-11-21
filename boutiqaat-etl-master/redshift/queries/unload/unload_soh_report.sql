UNLOAD ('select * from analytics.soh_report') 
to 's3://btq-etl/redshift/soh_report/soh_report_'
iam_role 'arn:aws:iam::652586300051:role/redshift-s3-writeAccess'
parallel off
ALLOWOVERWRITE
HEADER           
DELIMITER '\t'
maxfilesize 200 mb;