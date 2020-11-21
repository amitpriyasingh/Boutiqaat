BEGIN;
DROP TABLE IF EXISTS tmp_sessions_users_aggregated;
CREATE TEMP TABLE tmp_sessions_users_aggregated(
    advertising_id VARCHAR(100),
    email VARCHAR(100),
    first_name VARCHAR(100),
    last_name VARCHAR(100),
    gender VARCHAR(20),
    country VARCHAR(40), 
    city VARCHAR(40) 
);

copy tmp_sessions_users_aggregated from '{{S3PATH}}'
iam_role 'arn:aws:iam::652586300051:role/Redshift-athena-S3-readonly'
delimiter ','
region 'eu-west-1' 
CSV 
GZIP 
IGNOREHEADER 1 
emptyasnull
blanksasnull;

DROP TABLE IF EXISTS firebase.sessions_users_aggregated;
CREATE TABLE IF NOT EXISTS firebase.sessions_users_aggregated(
    advertising_id VARCHAR(100) ENCODE LZO,
    email VARCHAR(100) ENCODE LZO,
    first_name VARCHAR(100) ENCODE LZO,
    last_name VARCHAR(100) ENCODE LZO,
    gender VARCHAR(20) ENCODE LZO,
    country VARCHAR(40) ENCODE LZO,
    city VARCHAR(40) ENCODE LZO,
    sync_date TIMESTAMP ENCODE LZO
);

INSERT INTO firebase.sessions_users_aggregated 
select *, GETDATE() as sync_date from tmp_sessions_users_aggregated;
COMMIT;