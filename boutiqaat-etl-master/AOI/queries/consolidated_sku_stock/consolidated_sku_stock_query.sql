SELECT 
    sku,
    report_at_AST,
    nav_warehouse_sellable,
    nav_warehouse_not_sellable,
    nav_others_not_sellable,
    nav2crs_total,
    crs_reserved,
    crs_available,
    crs_actual_available,
    crs_force_soldout,
    crs_total,
    ofs_not_picked_or_cancelled,
    wh_reserved,
    nav_grn_pending_putaway,
    nav_return_pending_putaway,
    nav_bin_to_bin_movement,
    nav_total,
    crs_available_diff,
    crs_reserved_diff,
    soh
FROM aoi.consolidated_sku_stock
WHERE \$CONDITIONS