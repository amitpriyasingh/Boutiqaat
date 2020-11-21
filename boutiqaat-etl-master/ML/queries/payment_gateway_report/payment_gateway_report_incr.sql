BEGIN;
DROP TABLE IF EXISTS tmp_payment_gateway_report;
CREATE TEMP TABLE tmp_payment_gateway_report(
	report_date_utc TIMESTAMP NOT NULL,
	country	VARCHAR(5),
	device_type	VARCHAR(20),
	payment_gateway	VARCHAR(100),
	total_tx INTEGER,
	success_tx INTEGER,
	fail_tx INTEGER,
	user_closed_tx INTEGER,
	other_tx INTEGER,
	last_updated BIGINT
);

copy tmp_payment_gateway_report from '{{S3PATH}}'
iam_role 'arn:aws:iam::652586300051:role/Redshift-athena-S3-readonly'
delimiter '\t'
region 'eu-west-1' 
acceptinvchars
CSV 
GZIP 
emptyasnull
blanksasnull
NULL AS 'null';

DELETE FROM magento.payment_gateway_report where (report_date_utc,country,device_type,payment_gateway) in (SELECT report_date_utc,country,device_type,payment_gateway from tmp_payment_gateway_report)
INSERT INTO magento.payment_gateway_report
SELECT report_date_utc,country,device_type,payment_gateway,total_tx,success_tx,fail_tx,user_closed_tx,other_tx,last_updated
FROM tmp_payment_gateway_report;
COMMIT;
