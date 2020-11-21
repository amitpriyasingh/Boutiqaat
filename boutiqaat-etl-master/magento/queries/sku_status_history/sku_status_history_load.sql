DROP TABLE IF EXISTS tmp_sku_status_history;
CREATE TEMP TABLE tmp_sku_status_history(
    id INTEGER,
    sku VARCHAR(255),
    status BOOLEAN,
    user_id INTEGER,
    username VARCHAR(255),
    created_date TIMESTAMP
);

copy tmp_sku_status_history from '{{S3PATH}}'
iam_role 'arn:aws:iam::652586300051:role/Redshift-athena-S3-readonly'
delimiter '\t'
region 'eu-west-1' 
acceptinvchars
CSV 
GZIP 
emptyasnull
blanksasnull
NULL AS 'null';

CREATE TABLE IF NOT EXISTS magento.sku_status_history (
    id INTEGER,
    sku VARCHAR(255),
    status BOOLEAN,
    user_id INTEGER,
    username VARCHAR(255),
    created_date TIMESTAMP
);

BEGIN;
CALL public.drop_if_empty_tmp('tmp_sku_status_history');
DELETE FROM magento.sku_status_history WHERE 1=1;

INSERT INTO magento.sku_status_history
SELECT *
FROM tmp_sku_status_history;
COMMIT;
