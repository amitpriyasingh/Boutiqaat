DROP TABLE IF EXISTS tmp_catalog_product_flat_1;
CREATE TEMP TABLE tmp_catalog_product_flat_1(
    entity_id INTEGER,
    attribute_set_id SMALLINT,
    type_id VARCHAR(32),
    row_id INTEGER,
    accessories_size INTEGER,
    accessories_size_value VARCHAR(255),
    allow_open_amount INTEGER,
    boutiqaat_homepicks SMALLINT,
    cost DECIMAL(12,4),
    created_at TIMESTAMP,
    description VARCHAR(max),
    email_template VARCHAR(255),
    exclusive SMALLINT,
    footwear_size INTEGER,
    footwear_size_value VARCHAR(255),
    gender VARCHAR(255),
    giftcard_amounts DECIMAL(12,4),
    giftcard_type SMALLINT,
    gift_message_available SMALLINT,
    gift_wrapping_available SMALLINT,
    gift_wrapping_price DECIMAL(12,4),
    has_options SMALLINT,
    image_label VARCHAR(255),
    is_featured SMALLINT,
    is_new SMALLINT,
    is_redeemable INTEGER,
    lifetime INTEGER,
    links_exist INTEGER,
    links_purchased_separately INTEGER,
    links_title VARCHAR(255),
    lp_custom INTEGER,
    lp_custom_value VARCHAR(255),
    lp_custom_category INTEGER,
    lp_custom_category_value VARCHAR(255),
    manufacturer INTEGER,
    manufacturer_value VARCHAR(255),
    msrp DECIMAL(12,4),
    msrp_display_actual_price_type TEXT,
    name VARCHAR(255),
    news_from_date TIMESTAMP,
    news_to_date TIMESTAMP,
    open_amount_max DECIMAL(12,4),
    open_amount_min DECIMAL(12,4),
    ordered_qty VARCHAR(255),
    price DECIMAL(12,4),
    price_type INTEGER,
    price_view INTEGER,
    required_options SMALLINT,
    short_description VARCHAR(max),
    sku VARCHAR(64),
    sku_type INTEGER,
    small_image VARCHAR(255),
    small_image_label VARCHAR(255),
    special_from_date TIMESTAMP,
    special_price DECIMAL(12,4),
    special_to_date TIMESTAMP,
    swatch_image VARCHAR(255),
    tax_class_id INTEGER,
    thumbnail VARCHAR(255),
    thumbnail_label VARCHAR(255),
    updated_at TIMESTAMP,
    url_key VARCHAR(255),
    url_path VARCHAR(255),
    use_config_email_template INTEGER,
    use_config_is_redeemable INTEGER,
    use_config_lifetime INTEGER,
    visibility SMALLINT,
    weight DECIMAL(12,4),
    weight_type INTEGER
);

copy tmp_catalog_product_flat_1 from '{{S3PATH}}'
iam_role 'arn:aws:iam::652586300051:role/Redshift-athena-S3-readonly'
delimiter '\t'
region 'eu-west-1' 
acceptinvchars
CSV 
GZIP 
emptyasnull
blanksasnull
NULL AS 'null';

DROP TABLE IF EXISTS magento.catalog_product_flat_1;
CREATE TABLE magento.catalog_product_flat_1 (
    entity_id INTEGER NOT NULL ENCODE LZO DISTKEY SORTKEY PRIMARY KEY,
    attribute_set_id SMALLINT NOT NULL ENCODE DELTA,
    type_id VARCHAR(32) NOT NULL ENCODE LZO,
    row_id INTEGER NOT NULL ENCODE LZO,
    accessories_size INTEGER NULL ENCODE LZO,
    accessories_size_value VARCHAR(255) NULL ENCODE LZO,
    allow_open_amount INTEGER NULL ENCODE LZO,
    boutiqaat_homepicks SMALLINT NULL ENCODE DELTA,
    cost DECIMAL(12,4) NULL ENCODE LZO,
    created_at TIMESTAMP NULL ENCODE LZO,
    description VARCHAR(max) NULL ENCODE LZO,
    email_template VARCHAR(255) NULL ENCODE LZO,
    exclusive SMALLINT NULL ENCODE DELTA,
    footwear_size INTEGER NULL ENCODE LZO,
    footwear_size_value VARCHAR(255) NULL ENCODE LZO,
    gender VARCHAR(255) NULL ENCODE LZO,
    giftcard_amounts DECIMAL(12,4) NULL ENCODE LZO,
    giftcard_type SMALLINT NULL ENCODE DELTA,
    gift_message_available SMALLINT NULL ENCODE DELTA,
    gift_wrapping_available SMALLINT NULL ENCODE DELTA,
    gift_wrapping_price DECIMAL(12,4) NULL ENCODE LZO,
    has_options SMALLINT NOT NULL ENCODE DELTA,
    image_label VARCHAR(255) NULL ENCODE LZO,
    is_featured SMALLINT NULL ENCODE DELTA,
    is_new SMALLINT NULL ENCODE DELTA,
    is_redeemable INTEGER NULL ENCODE LZO,
    lifetime INTEGER NULL ENCODE LZO,
    links_exist INTEGER NULL ENCODE LZO,
    links_purchased_separately INTEGER NULL ENCODE LZO,
    links_title VARCHAR(255) NULL ENCODE LZO,
    lp_custom INTEGER NULL ENCODE LZO,
    lp_custom_value VARCHAR(255) NULL ENCODE LZO,
    lp_custom_category INTEGER NULL ENCODE LZO,
    lp_custom_category_value VARCHAR(255) NULL ENCODE LZO,
    manufacturer INTEGER NULL ENCODE LZO,
    manufacturer_value VARCHAR(255) NULL ENCODE LZO,
    msrp DECIMAL(12,4) NULL ENCODE LZO,
    msrp_display_actual_price_type TEXT NULL ENCODE LZO,
    name VARCHAR(255) NULL ENCODE LZO,
    news_from_date TIMESTAMP NULL ENCODE LZO,
    news_to_date TIMESTAMP NULL ENCODE LZO,
    open_amount_max DECIMAL(12,4) NULL ENCODE LZO,
    open_amount_min DECIMAL(12,4) NULL ENCODE LZO,
    ordered_qty VARCHAR(255) NULL ENCODE LZO,
    price DECIMAL(12,4) NULL ENCODE LZO,
    price_type INTEGER NULL ENCODE LZO,
    price_view INTEGER NULL ENCODE LZO,
    required_options SMALLINT NOT NULL ENCODE DELTA,
    short_description VARCHAR(max) NULL ENCODE LZO,
    sku VARCHAR(64) NULL ENCODE LZO,
    sku_type INTEGER NULL ENCODE LZO,
    small_image VARCHAR(255) NULL ENCODE LZO,
    small_image_label VARCHAR(255) NULL ENCODE LZO,
    special_from_date TIMESTAMP NULL ENCODE LZO,
    special_price DECIMAL(12,4) NULL ENCODE LZO,
    special_to_date TIMESTAMP NULL ENCODE LZO,
    swatch_image VARCHAR(255) NULL ENCODE LZO,
    tax_class_id INTEGER NULL ENCODE LZO,
    thumbnail VARCHAR(255) NULL ENCODE LZO,
    thumbnail_label VARCHAR(255) NULL ENCODE LZO,
    updated_at TIMESTAMP NOT NULL ENCODE LZO,
    url_key VARCHAR(255) NULL ENCODE LZO,
    url_path VARCHAR(255) NULL ENCODE LZO,
    use_config_email_template INTEGER NULL ENCODE LZO,
    use_config_is_redeemable INTEGER NULL ENCODE LZO,
    use_config_lifetime INTEGER NULL ENCODE LZO,
    visibility SMALLINT NULL ENCODE DELTA,
    weight DECIMAL(12,4) NULL ENCODE LZO,
    weight_type INTEGER NULL ENCODE LZO
);

INSERT INTO magento.catalog_product_flat_1
SELECT entity_id,attribute_set_id,type_id,row_id,accessories_size,accessories_size_value,allow_open_amount,boutiqaat_homepicks,cost,created_at,description,email_template,exclusive,footwear_size,footwear_size_value,gender,giftcard_amounts,giftcard_type,gift_message_available,gift_wrapping_available,gift_wrapping_price,has_options,image_label,is_featured,is_new,is_redeemable,lifetime,links_exist,links_purchased_separately,links_title,lp_custom,lp_custom_value,lp_custom_category,lp_custom_category_value,manufacturer,manufacturer_value,msrp,msrp_display_actual_price_type,name,news_from_date,news_to_date,open_amount_max,open_amount_min,ordered_qty,price,price_type,price_view,required_options,short_description,sku,sku_type,small_image,small_image_label,special_from_date,special_price,special_to_date,swatch_image,tax_class_id,thumbnail,thumbnail_label,updated_at,url_key,url_path,use_config_email_template,use_config_is_redeemable,use_config_lifetime,visibility,weight,weight_type
FROM tmp_catalog_product_flat_1;
COMMIT;
