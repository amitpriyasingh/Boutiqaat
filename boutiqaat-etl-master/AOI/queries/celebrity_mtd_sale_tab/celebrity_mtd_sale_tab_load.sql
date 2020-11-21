BEGIN;

DROP TABLE IF EXISTS tmp_celebrity_mtd_sale_tab;
CREATE TEMP TABLE tmp_celebrity_mtd_sale_tab(
    target_month INTEGER,
    max_order_at TIMESTAMP,
    max_item_id INTEGER,
    celebrity_id INTEGER,
    celebrity_name VARCHAR(100),
    code VARCHAR(10),
    celeb_mtd_sale DECIMAL(10,3),
    celeb_mtd_target DECIMAL(10,3),
    celeb_total_target DECIMAL(10,3),
    account_manager VARCHAR(250),
    am_email VARCHAR(100),
    rm_email VARCHAR(100),
    am_celeb_count BIGINT
);

copy tmp_celebrity_mtd_sale_tab from '{{S3PATH}}'
iam_role 'arn:aws:iam::652586300051:role/Redshift-athena-S3-readonly'
delimiter '\t'
region 'eu-west-1' 
acceptinvchars
CSV 
GZIP 
emptyasnull
blanksasnull
NULL AS 'null';

CREATE TABLE IF NOT EXISTS aoi.celebrity_mtd_sale_tab (
      target_month INTEGER NOT NULL ENCODE LZO,
      max_order_at TIMESTAMP NULL ENCODE LZO,
      max_item_id INTEGER NULL ENCODE LZO,
      celebrity_id INTEGER NOT NULL ENCODE LZO DISTKEY SORTKEY,
      celebrity_name VARCHAR(100) NULL ENCODE LZO,
      code VARCHAR(10) NULL ENCODE LZO,
      celeb_mtd_sale DECIMAL(10,3) NULL ENCODE LZO,
      celeb_mtd_target DECIMAL(10,3) NULL ENCODE LZO,
      celeb_total_target DECIMAL(10,3) NULL ENCODE LZO,
      account_manager VARCHAR(250) NULL ENCODE LZO,
      am_email VARCHAR(100) NOT NULL ENCODE LZO,
      rm_email VARCHAR(100) NULL ENCODE LZO,
      am_celeb_count BIGINT ENCODE LZO
);

DELETE FROM aoi.celebrity_mtd_sale_tab WHERE 1=1;

INSERT INTO aoi.celebrity_mtd_sale_tab
SELECT target_month, max_order_at, max_item_id, celebrity_id, celebrity_name, code, celeb_mtd_sale, celeb_mtd_target, celeb_total_target, account_manager, am_email, rm_email, am_celeb_count 
FROM tmp_celebrity_mtd_sale_tab;


COMMIT;
