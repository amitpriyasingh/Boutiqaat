DROP TABLE IF EXISTS tmp_location;
CREATE TEMP TABLE tmp_location(
	ts VARCHAR(40),
    code VARCHAR(10),
    name VARCHAR(50),
    default_bin_code VARCHAR(20),
    name_2 VARCHAR(50),
    address VARCHAR(50),
    address_2 VARCHAR(50),
    city VARCHAR(30),
    phone_no VARCHAR(30),
    phone_no_2 VARCHAR(30),
    telex_no VARCHAR(30),
    fax_no VARCHAR(30),
    contact VARCHAR(50),
    post_code VARCHAR(20),
    county VARCHAR(30),
    e_mail VARCHAR(80),
    home_page VARCHAR(90),
    country_region_code VARCHAR(10),
    use_as_in_transit SMALLINT,
    require_put_away SMALLINT,
    require_pick SMALLINT,
    cross_dock_due_date_calc varchar(32),
    use_cross_docking SMALLINT,
    require_receive SMALLINT,
    require_shipment SMALLINT,
    bin_mandatory SMALLINT,
    directed_put_away_and_pick SMALLINT,
    default_bin_selection INTEGER,
    outbound_whse_handling_time varchar(32),
    inbound_whse_handling_time varchar(32),
    put_away_template_code VARCHAR(10),
    use_put_away_worksheet SMALLINT,
    pick_according_to_fefo SMALLINT,
    allow_breakbulk SMALLINT,
    bin_capacity_policy INTEGER,
    open_shop_floor_bin_code VARCHAR(20),
    to_production_bin_code VARCHAR(20),
    from_production_bin_code VARCHAR(20),
    adjustment_bin_code VARCHAR(20),
    always_create_put_away_line SMALLINT,
    always_create_pick_line SMALLINT,
    special_equipment INTEGER,
    receipt_bin_code VARCHAR(20),
    shipment_bin_code VARCHAR(20),
    cross_dock_bin_code VARCHAR(20),
    to_assembly_bin_code VARCHAR(20),
    from_assembly_bin_code VARCHAR(20),
    asm_to_order_shpt_bin_code VARCHAR(20),
    base_calendar_code VARCHAR(10),
    use_adcs SMALLINT,
    allowed_pick_lines DECIMAL(38,20),
    allowed_put_away_line DECIMAL(38,20),
    pna_count INTEGER,
    pass_bin VARCHAR(20),
    fail_bin VARCHAR(20),
    excess_bin VARCHAR(20),
    put_away_bin_suggetion_wms SMALLINT,
    pick_bin_suggetion_wms SMALLINT,
    stock_update SMALLINT,
    assembly_rm_tolerance DECIMAL(38,20),
    suggest_serial_no SMALLINT,
    fail_bin_for_putaway VARCHAR(20),
    priority INTEGER,
    lot_no VARCHAR(20),
    default_bin VARCHAR(20),
    staging_bin VARCHAR(20),
    minimum_expiry_period TIMESTAMP,
    zone_code VARCHAR(20),
    bin_type_code VARCHAR(20),
    return_pass_bin VARCHAR(20),
    return_fail_bin VARCHAR(20),
    return_refurbish_bin VARCHAR(20)
);

copy tmp_location from '{{S3PATH}}'
iam_role 'arn:aws:iam::652586300051:role/Redshift-athena-S3-readonly'
delimiter '\t'
region 'eu-west-1' 
acceptinvchars
CSV 
GZIP 
NULL AS 'null';

BEGIN;
DROP TABLE IF EXISTS NAV.location;
CREATE TABLE NAV.location (
    ts VARCHAR(40) NOT NULL ENCODE LZO,
    code VARCHAR(10) NOT NULL ENCODE LZO PRIMARY KEY,
    name VARCHAR(50) NOT NULL ENCODE LZO,
    default_bin_code VARCHAR(20) NOT NULL ENCODE LZO,
    name_2 VARCHAR(50) NOT NULL ENCODE LZO,
    address VARCHAR(50) NOT NULL ENCODE LZO,
    address_2 VARCHAR(50) NOT NULL ENCODE LZO,
    city VARCHAR(30) NOT NULL ENCODE LZO,
    phone_no VARCHAR(30) NOT NULL ENCODE LZO,
    phone_no_2 VARCHAR(30) NOT NULL ENCODE LZO,
    telex_no VARCHAR(30) NOT NULL ENCODE LZO,
    fax_no VARCHAR(30) NOT NULL ENCODE LZO,
    contact VARCHAR(50) NOT NULL ENCODE LZO,
    post_code VARCHAR(20) NOT NULL ENCODE LZO,
    county VARCHAR(30) NOT NULL ENCODE LZO,
    e_mail VARCHAR(80) NOT NULL ENCODE LZO,
    home_page VARCHAR(90) NOT NULL ENCODE LZO,
    country_region_code VARCHAR(10) NOT NULL ENCODE LZO,
    use_as_in_transit SMALLINT NOT NULL ENCODE DELTA,
    require_put_away SMALLINT NOT NULL ENCODE DELTA,
    require_pick SMALLINT NOT NULL ENCODE DELTA,
    cross_dock_due_date_calc VARCHAR(32) NOT NULL ENCODE LZO,
    use_cross_docking SMALLINT NOT NULL ENCODE DELTA,
    require_receive SMALLINT NOT NULL ENCODE DELTA,
    require_shipment SMALLINT NOT NULL ENCODE DELTA,
    bin_mandatory SMALLINT NOT NULL ENCODE DELTA,
    directed_put_away_and_pick SMALLINT NOT NULL ENCODE DELTA,
    default_bin_selection INTEGER NOT NULL ENCODE DELTA,
    outbound_whse_handling_time VARCHAR(32) NOT NULL ENCODE LZO,
    inbound_whse_handling_time VARCHAR(32) NOT NULL ENCODE LZO,
    put_away_template_code VARCHAR(10) NOT NULL ENCODE LZO,
    use_put_away_worksheet SMALLINT NOT NULL ENCODE DELTA,
    pick_according_to_fefo SMALLINT NOT NULL ENCODE DELTA,
    allow_breakbulk SMALLINT NOT NULL ENCODE DELTA,
    bin_capacity_policy INTEGER NOT NULL ENCODE DELTA,
    open_shop_floor_bin_code VARCHAR(20) NOT NULL ENCODE LZO,
    to_production_bin_code VARCHAR(20) NOT NULL ENCODE LZO,
    from_production_bin_code VARCHAR(20) NOT NULL ENCODE LZO,
    adjustment_bin_code VARCHAR(20) NOT NULL ENCODE LZO,
    always_create_put_away_line SMALLINT NOT NULL ENCODE DELTA,
    always_create_pick_line SMALLINT NOT NULL ENCODE DELTA,
    special_equipment INTEGER NOT NULL ENCODE DELTA,
    receipt_bin_code VARCHAR(20) NOT NULL ENCODE LZO,
    shipment_bin_code VARCHAR(20) NOT NULL ENCODE LZO,
    cross_dock_bin_code VARCHAR(20) NOT NULL ENCODE LZO,
    to_assembly_bin_code VARCHAR(20) NOT NULL ENCODE LZO,
    from_assembly_bin_code VARCHAR(20) NOT NULL ENCODE LZO,
    asm_to_order_shpt_bin_code VARCHAR(20) NOT NULL ENCODE LZO,
    base_calendar_code VARCHAR(10) NOT NULL ENCODE LZO,
    use_adcs SMALLINT NOT NULL ENCODE DELTA,
    allowed_pick_lines DECIMAL(38,20) NOT NULL ENCODE LZO,
    allowed_put_away_line DECIMAL(38,20) NOT NULL ENCODE LZO,
    pna_count INTEGER NOT NULL ENCODE DELTA,
    pass_bin VARCHAR(20) NOT NULL ENCODE LZO,
    fail_bin VARCHAR(20) NOT NULL ENCODE LZO,
    excess_bin VARCHAR(20) NOT NULL ENCODE LZO,
    put_away_bin_suggetion_wms SMALLINT NOT NULL ENCODE DELTA,
    pick_bin_suggetion_wms SMALLINT NOT NULL ENCODE DELTA,
    stock_update SMALLINT NOT NULL ENCODE DELTA,
    assembly_rm_tolerance DECIMAL(38,20) NOT NULL ENCODE LZO,
    suggest_serial_no SMALLINT NOT NULL ENCODE DELTA,
    fail_bin_for_putaway VARCHAR(20) NOT NULL ENCODE LZO,
    priority INTEGER NOT NULL ENCODE DELTA,
    lot_no VARCHAR(20) NOT NULL ENCODE LZO,
    default_bin VARCHAR(20) NOT NULL ENCODE LZO,
    staging_bin VARCHAR(20) NOT NULL ENCODE LZO,
    minimum_expiry_period TIMESTAMP NOT NULL ENCODE LZO,
    zone_code VARCHAR(20) NOT NULL ENCODE LZO,
    bin_type_code VARCHAR(20) NOT NULL ENCODE LZO,
    return_pass_bin VARCHAR(20) NOT NULL ENCODE LZO,
    return_fail_bin VARCHAR(20) NOT NULL ENCODE LZO,
    return_refurbish_bin VARCHAR(20) NOT NULL ENCODE LZO
);


INSERT INTO NAV.location 
SELECT ts, code, name, default_bin_code, name_2, address, address_2, city, phone_no, phone_no_2, telex_no, fax_no, contact, post_code, county, e_mail, home_page, country_region_code, use_as_in_transit, require_put_away, require_pick, cross_dock_due_date_calc, use_cross_docking, require_receive, require_shipment, bin_mandatory, directed_put_away_and_pick, default_bin_selection, outbound_whse_handling_time, inbound_whse_handling_time, put_away_template_code, use_put_away_worksheet, pick_according_to_fefo, allow_breakbulk, bin_capacity_policy, open_shop_floor_bin_code, to_production_bin_code, from_production_bin_code, adjustment_bin_code, always_create_put_away_line, always_create_pick_line, special_equipment, receipt_bin_code, shipment_bin_code, cross_dock_bin_code, to_assembly_bin_code, from_assembly_bin_code, asm_to_order_shpt_bin_code, base_calendar_code, use_adcs, allowed_pick_lines, allowed_put_away_line, pna_count, pass_bin, fail_bin, excess_bin, put_away_bin_suggetion_wms, pick_bin_suggetion_wms, stock_update, assembly_rm_tolerance, suggest_serial_no, fail_bin_for_putaway, priority, lot_no, default_bin, staging_bin, minimum_expiry_period, zone_code, bin_type_code, return_pass_bin, return_fail_bin, return_refurbish_bin 
FROM tmp_location;
COMMIT;