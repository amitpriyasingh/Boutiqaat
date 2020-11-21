BEGIN;
DROP TABLE IF EXISTS tmp_stock_details;
CREATE TEMP TABLE tmp_stock_details(
	ts VARCHAR(40),
	entry_no INTEGER,
	item_no VARCHAR(50),
	location VARCHAR(20),
	qty_in_stock INTEGER,
	stock_type INTEGER,
	stock_sync_erp SMALLINT,
	stock_sync_erp_error VARCHAR(250),
	stock_sync_erp_at TIMESTAMP,
	stock_sync_web SMALLINT,
	stock_sync_web_at TIMESTAMP,
	stock_sync_web_error VARCHAR(250),
	insert_at TIMESTAMP,
	stock_sync_newstack SMALLINT,
	stock_sync_newstack_at TIMESTAMP,
	stock_sync_newstack_msg VARCHAR(250),
	stock_sync_newstack_msg_1 VARCHAR(250),
	retry_count_web INTEGER,
	retry_count_newstack INTEGER,
	retry_count_erp INTEGER,
	warehouse_entry INTEGER,
	warehouse_jn INTEGER,
	warehouse_activity_line INTEGER,
	warehouse_activity_line_2 INTEGER,
	movement_journal_line INTEGER,
	reserved_quantity INTEGER
);

copy tmp_stock_details from '{{S3PATH}}'
iam_role 'arn:aws:iam::652586300051:role/Redshift-athena-S3-readonly'
delimiter '\t'
region 'eu-west-1' 
acceptinvchars
CSV 
GZIP 
NULL AS 'null';


DROP TABLE IF EXISTS NAV.stock_details;
CREATE TABLE NAV.stock_details (
	ts VARCHAR(40) NOT NULL ENCODE LZO,
	entry_no INTEGER NOT NULL ENCODE DELTA DISTKEY SORTKEY PRIMARY KEY,
	item_no VARCHAR(50) NOT NULL ENCODE LZO,
	location VARCHAR(20) NOT NULL ENCODE LZO,
	qty_in_stock INTEGER NOT NULL ENCODE DELTA,
	stock_type INTEGER NOT NULL ENCODE DELTA,
	stock_sync_erp SMALLINT NOT NULL ENCODE DELTA,
	stock_sync_erp_error VARCHAR(250) NOT NULL ENCODE LZO,
	stock_sync_erp_at TIMESTAMP NOT NULL ENCODE LZO,
	stock_sync_web SMALLINT NOT NULL ENCODE DELTA,
	stock_sync_web_at TIMESTAMP NOT NULL ENCODE LZO,
	stock_sync_web_error VARCHAR(250) NOT NULL ENCODE LZO,
	insert_at TIMESTAMP NOT NULL ENCODE LZO,
	stock_sync_newstack SMALLINT NOT NULL ENCODE DELTA,
	stock_sync_newstack_at TIMESTAMP NOT NULL ENCODE LZO,
	stock_sync_newstack_msg VARCHAR(250) NOT NULL ENCODE LZO,
	stock_sync_newstack_msg_1 VARCHAR(250) NOT NULL ENCODE LZO,
	retry_count_web INTEGER NOT NULL ENCODE DELTA,
	retry_count_newstack INTEGER NOT NULL ENCODE DELTA,
	retry_count_erp INTEGER NOT NULL ENCODE DELTA,
	warehouse_entry INTEGER NOT NULL ENCODE DELTA,
	warehouse_jn INTEGER NOT NULL ENCODE DELTA,
	warehouse_activity_line INTEGER NOT NULL ENCODE DELTA,
	warehouse_activity_line_2 INTEGER NOT NULL ENCODE DELTA,
	movement_journal_line INTEGER NOT NULL ENCODE DELTA,
	reserved_quantity INTEGER NOT NULL ENCODE DELTA
);


INSERT INTO NAV.stock_details 
SELECT ts, entry_no, item_no, location, qty_in_stock, stock_type, stock_sync_erp, stock_sync_erp_error, stock_sync_erp_at, stock_sync_web, stock_sync_web_at, stock_sync_web_error, insert_at, stock_sync_newstack, stock_sync_newstack_at, stock_sync_newstack_msg, stock_sync_newstack_msg_1, retry_count_web, retry_count_newstack, retry_count_erp, warehouse_entry, warehouse_jn, warehouse_activity_line, warehouse_activity_line_2, movement_journal_line, reserved_quantity
FROM tmp_stock_details;
COMMIT;