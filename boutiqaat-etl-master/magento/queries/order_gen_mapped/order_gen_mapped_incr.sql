BEGIN;
DROP TABLE IF EXISTS tmp_order_gen_mapped;
CREATE TEMP TABLE tmp_order_gen_mapped(
			order_number BIGINT,
			gender integer,
			email  varchar(255)
);

copy tmp_order_gen_mapped from '{{S3PATH}}'
iam_role 'arn:aws:iam::652586300051:role/Redshift-athena-S3-readonly'
delimiter '\t'
region 'eu-west-1' 
acceptinvchars
CSV 
GZIP 
emptyasnull
blanksasnull
NULL AS 'null';

INSERT INTO magento.order_gen_mapped
SELECT *
FROM tmp_order_gen_mapped;
COMMIT;
