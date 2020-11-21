BEGIN;
DROP TABLE IF EXISTS tmp_warehouse_entry;
CREATE TEMP TABLE tmp_warehouse_entry(
    rownum INTEGER,
	ts VARCHAR(40),
	entry_no decimal(10,2),
	journal_batch_name varchar(40),
	line_no integer,
	registering_date timestamp,
	location_code varchar(40),
	zone_code varchar(20),
	bin_code varchar(40),
	description varchar(400),
	item_no varchar(40),
	quantity decimal(38,20),
	qty_base decimal(38,20),
	source_type integer,
	source_subtype integer,
	source_no varchar(40),
	source_line_no integer,
	source_subline_no integer,
	source_document integer,
	source_code varchar(20),
	reason_code varchar(20),
	no_series varchar(20),
	bin_type_code varchar(20),
	cubage decimal(38,20),
	weight decimal(38,20),
	journal_template_name varchar(20),
	whse_document_no varchar(40),
	whse_document_type integer,
	whse_document_line_no integer,
	entry_type integer,
	reference_document integer,
	reference_no varchar(40),
	user_id varchar(100),
	variant_code varchar(20),
	qty_per_unit_ofmeasure decimal(38,20),
	unit_of_measure_code varchar(20),
	serial_no varchar(40),
	lot_no varchar(40),
	warranty_date timestamp,
	expiration_date timestamp,
	phys_invt_counting_period_code varchar(20),
	phys_invt_counting_period_type integer,
	dedicated integer,
	posted_in_queue integer,
	insert_datetime timestamp,
	batch_no varchar(20),
	closed integer,
	adjustment integer
 );

copy tmp_warehouse_entry from '{{S3PATH}}'
iam_role 'arn:aws:iam::652586300051:role/Redshift-athena-S3-readonly'
delimiter '\t'
region 'eu-west-1' 
acceptinvchars
CSV 
GZIP 
NULL AS 'null';


INSERT INTO NAV.warehouse_entry
SELECT ts, entry_no, journal_batch_name, line_no, registering_date, location_code, zone_code, bin_code, description, item_no, quantity, qty_base, source_type, source_subtype, source_no, source_line_no, source_subline_no, source_document, source_code, reason_code, no_series, bin_type_code, cubage, weight, journal_template_name, whse_document_no, whse_document_type, whse_document_line_no, entry_type, reference_document, reference_no, user_id, variant_code, qty_per_unit_ofmeasure, unit_of_measure_code, serial_no, lot_no, warranty_date, expiration_date, phys_invt_counting_period_code, phys_invt_counting_period_type, dedicated, posted_in_queue, insert_datetime, batch_no, closed, adjustment
FROM tmp_warehouse_entry;
COMMIT;