BEGIN;
DROP TABLE IF EXISTS tmp_celebrity_master;
CREATE TEMP TABLE tmp_celebrity_master(
    celebrity_id INTEGER,
    celebrity_code VARCHAR(100),
    celebrity_status BOOLEAN,
    user_id INTEGER,
    celebrity_name VARCHAR(100),
    celebrity_arabic_name VARCHAR(100),
    celebrity_description VARCHAR(2000),
    celebrity_arabic_description VARCHAR(2000),
    meta_keyword VARCHAR(max),
    meta_description VARCHAR(2000),
    boutique_name VARCHAR(100),
    boutique_arabic_name VARCHAR(100),
    erp_account_number INTEGER,
    nationality VARCHAR(100),
    include_home_slider BOOLEAN,
    mobile_banner_image_arabic VARCHAR(100),
    banner_image_arabic VARCHAR(100),
    main_image VARCHAR(100),
    main_image_arabic VARCHAR(255),
    banner_image VARCHAR(100),
    web_banner_image VARCHAR(100),
    web_banner_url VARCHAR(100),
    mobile_banner_image VARCHAR(100),
    main_image_v2 VARCHAR(255),
    banner_image_v2 VARCHAR(255),
    banner_image_arabic_v2 VARCHAR(255),
    banner_url_value VARCHAR(100),
    banner_url_key VARCHAR(100),
    banner_filter_key VARCHAR(100),
    banner_filter_value VARCHAR(100),
    celebrity_email_id VARCHAR(100),
    instagram VARCHAR(100),
    facebook VARCHAR(100),
    snapchat VARCHAR(100),
    twitter VARCHAR(100),
    last_ad_number INTEGER,
    store_ids VARCHAR(100),
    created_at timestamp,
    updated_at timestamp,
    celebrity_cache_expiration_time INTEGER,
    celebrity_gender INTEGER,
    celebrity_type INTEGER
);

copy tmp_celebrity_master from '{{S3PATH}}'
iam_role 'arn:aws:iam::652586300051:role/Redshift-athena-S3-readonly'
delimiter '\t'
region 'eu-west-1' 
acceptinvchars
CSV 
GZIP 
emptyasnull
blanksasnull
NULL AS 'null';

CREATE TABLE IF NOT EXISTS aoi.celebrity_master (
    celebrity_id INTEGER NULL,
    celebrity_code VARCHAR(100) NULL,
    celebrity_status BOOLEAN NULL,
    user_id INTEGER NULL,
    celebrity_name VARCHAR(100) NULL,
    celebrity_arabic_name VARCHAR(100) NULL,
    celebrity_description VARCHAR(2000) NULL,
    celebrity_arabic_description VARCHAR(2000) NULL,
    meta_keyword VARCHAR(max) NULL,
    meta_description VARCHAR(2000) NULL,
    boutique_name VARCHAR(100) NULL,
    boutique_arabic_name VARCHAR(100) NULL,
    erp_account_number INTEGER NULL,
    nationality VARCHAR(100) NULL,
    include_home_slider BOOLEAN NULL,
    mobile_banner_image_arabic VARCHAR(100) NULL,
    banner_image_arabic VARCHAR(100) NULL,
    main_image VARCHAR(100) NULL,
    main_image_arabic VARCHAR(255) NULL,
    banner_image VARCHAR(100) NULL,
    web_banner_image VARCHAR(100) NULL,
    web_banner_url VARCHAR(100) NULL,
    mobile_banner_image VARCHAR(100) NULL,
    main_image_v2 VARCHAR(255) NULL,
    banner_image_v2 VARCHAR(255) NULL,
    banner_image_arabic_v2 VARCHAR(255) NULL,
    banner_url_value VARCHAR(100) NULL,
    banner_url_key VARCHAR(100) NULL,
    banner_filter_key VARCHAR(100) NULL,
    banner_filter_value VARCHAR(100) NULL,
    celebrity_email_id VARCHAR(100) NULL,
    instagram VARCHAR(100) NULL,
    facebook VARCHAR(100) NULL,
    snapchat VARCHAR(100) NULL,
    twitter VARCHAR(100) NULL,
    last_ad_number INTEGER NULL,
    store_ids VARCHAR(100) NULL,
    created_at timestamp NULL,
    updated_at timestamp NULL,
    celebrity_cache_expiration_time INTEGER NULL,
    celebrity_gender INTEGER NULL,
    celebrity_type INTEGER NULL
);

DELETE FROM aoi.celebrity_master WHERE 1=1;

INSERT INTO aoi.celebrity_master
SELECT celebrity_id,celebrity_code,celebrity_status,user_id,celebrity_name,celebrity_arabic_name,celebrity_description,celebrity_arabic_description,meta_keyword,meta_description,boutique_name,boutique_arabic_name,erp_account_number,nationality,include_home_slider,mobile_banner_image_arabic,banner_image_arabic,main_image,main_image_arabic,banner_image,web_banner_image,web_banner_url,mobile_banner_image,main_image_v2,banner_image_v2,banner_image_arabic_v2,banner_url_value,banner_url_key,banner_filter_key,banner_filter_value,celebrity_email_id,instagram,facebook,snapchat,twitter,last_ad_number,store_ids,created_at,updated_at,celebrity_cache_expiration_time,celebrity_gender,celebrity_type
FROM tmp_celebrity_master;


COMMIT;
