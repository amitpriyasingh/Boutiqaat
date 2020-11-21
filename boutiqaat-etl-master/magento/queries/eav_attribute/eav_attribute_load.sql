BEGIN;
DROP TABLE IF EXISTS tmp_eav_attribute;
CREATE TEMP TABLE tmp_eav_attribute(
	attribute_id SMALLINT,
	entity_type_id SMALLINT,
	attribute_code VARCHAR(255),
	attribute_model VARCHAR(255),
	backend_model VARCHAR(255),
	backend_type VARCHAR(8),
	backend_table VARCHAR(255),
	frontend_model VARCHAR(255),
	frontend_input VARCHAR(50),
	frontend_label VARCHAR(255),
	frontend_class VARCHAR(255),
	source_model VARCHAR(255),
	is_required SMALLINT,
	is_user_defined SMALLINT,
	default_value TEXT,
	is_unique SMALLINT,
	note VARCHAR(255)
);

copy tmp_eav_attribute from '{{S3PATH}}'
iam_role 'arn:aws:iam::652586300051:role/Redshift-athena-S3-readonly'
delimiter '\t'
region 'eu-west-1' 
acceptinvchars
CSV 
GZIP 
emptyasnull
blanksasnull
NULL AS 'null';

DROP TABLE IF EXISTS magento.eav_attribute;
CREATE TABLE magento.eav_attribute (
	attribute_id SMALLINT NOT NULL ENCODE DELTA SORTKEY DISTKEY PRIMARY KEY,
	entity_type_id SMALLINT NOT NULL ENCODE DELTA,
	attribute_code VARCHAR(255) NOT NULL ENCODE LZO,
	attribute_model VARCHAR(255) NULL ENCODE LZO,
	backend_model VARCHAR(255) NULL ENCODE LZO,
	backend_type VARCHAR(8) NOT NULL ENCODE LZO,
	backend_table VARCHAR(255) NULL ENCODE LZO,
	frontend_model VARCHAR(255) NULL ENCODE LZO,
	frontend_input VARCHAR(50) NULL ENCODE LZO,
	frontend_label VARCHAR(255) NULL ENCODE LZO,
	frontend_class VARCHAR(255) NULL ENCODE LZO,
	source_model VARCHAR(255) NULL ENCODE LZO,
	is_required SMALLINT NOT NULL ENCODE DELTA,
	is_user_defined SMALLINT NOT NULL ENCODE DELTA,
	default_value TEXT NULL ENCODE LZO,
	is_unique SMALLINT NOT NULL ENCODE DELTA,
	note VARCHAR(255) NULL ENCODE LZO
);

INSERT INTO magento.eav_attribute
SELECT attribute_id,entity_type_id,attribute_code,attribute_model,backend_model,backend_type,backend_table,frontend_model,frontend_input,frontend_label,frontend_class,source_model,is_required,is_user_defined,default_value,is_unique,note
FROM tmp_eav_attribute;
COMMIT;
