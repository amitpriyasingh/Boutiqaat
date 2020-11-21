DROP TABLE IF EXISTS tmp_eav_attribute_option_value;
CREATE TEMP TABLE tmp_eav_attribute_option_value(
	value_id INTEGER,
	option_id INTEGER,
	store_id SMALLINT,
	value VARCHAR(255)
);

copy tmp_eav_attribute_option_value from '{{S3PATH}}'
iam_role 'arn:aws:iam::652586300051:role/Redshift-athena-S3-readonly'
delimiter '\t'
region 'eu-west-1' 
acceptinvchars
CSV 
GZIP 
emptyasnull
blanksasnull
NULL AS 'null';

CREATE TABLE IF NOT EXISTS magento.eav_attribute_option_value (
	value_id INTEGER NOT NULL ENCODE LZO PRIMARY KEY,
	option_id INTEGER NOT NULL ENCODE LZO DISTKEY SORTKEY,
	store_id SMALLINT NOT NULL ENCODE DELTA,
	value VARCHAR(255) NULL ENCODE LZO
);

BEGIN;
CALL public.drop_if_empty_tmp('tmp_eav_attribute_option_value');
DELETE FROM magento.eav_attribute_option_value WHERE 1=1;

INSERT INTO magento.eav_attribute_option_value
SELECT value_id,option_id,store_id,value
FROM tmp_eav_attribute_option_value;
COMMIT;
