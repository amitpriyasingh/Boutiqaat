BEGIN;
DROP TABLE IF EXISTS tmp_eav_attribute_option_swatch;
CREATE TEMP TABLE tmp_eav_attribute_option_swatch(
	swatch_id INTEGER,
	option_id INTEGER,
	store_id SMALLINT,
	type SMALLINT,
	value VARCHAR(255),
);

copy tmp_eav_attribute_option_swatch from '{{S3PATH}}'
iam_role 'arn:aws:iam::652586300051:role/Redshift-athena-S3-readonly'
delimiter '\t'
region 'eu-west-1' 
acceptinvchars
CSV 
GZIP 
emptyasnull
blanksasnull
NULL AS 'null';

DROP TABLE IF EXISTS magento.eav_attribute_option_swatch;
CREATE TABLE magento.eav_attribute_option_swatch (
	swatch_id INTEGER NOT NULL ENCODE LZO DISTKEY SORTKEY PRIMARY KEY,
	option_id INTEGER NOT NULL ENCODE LZO,
	store_id SMALLINT NOT NULL ENCODE DELTA,
	type SMALLINT NOT NULL ENCODE DELTA,
	value VARCHAR(255) NULL ENCODE LZO
);

INSERT INTO magento.eav_attribute_option_swatch
SELECT swatch_id,option_id,store_id,type,value
FROM tmp_eav_attribute_option_swatch;
COMMIT;
