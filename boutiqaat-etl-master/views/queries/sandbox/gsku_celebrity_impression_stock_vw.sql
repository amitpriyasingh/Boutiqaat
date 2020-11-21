CREATE OR REPLACE VIEW sandbox.gsku_celebrity_impression_stock_vw AS
SELECT COALESCE(c.sku, m.sku) AS sku, 0 AS celebrity_id, d.enable_date, q.first_sell_date, COALESCE(c.first_grn_date, q.first_grn_date) AS first_grn_date, c.brand, c.child_sku, c.product_status, c.category1, c.category2, c.category3, c.category4, COALESCE(m.impression_7days, 0::bigint) AS impression_7days, COALESCE(m.impression_14days, 0::bigint) AS impression_14days, COALESCE(m.impression_28days, 0::bigint) AS impression_28days, COALESCE(m.impression_90days, 0::bigint) AS impression_90days, m.qty_7days, m.qty_14days, m.qty_28days, m.qty_90days, COALESCE(m.rev_7days, 0::numeric) AS rev_7days, COALESCE(m.rev_14days, 0::numeric) AS rev_14days, COALESCE(m.rev_28days, 0::numeric) AS rev_28days, COALESCE(m.rev_90days, 0::numeric) AS rev_90days, q.soh, q.open_po_qty, q.stock_cover, q.open_po_flag
   FROM ( SELECT COALESCE(c.sku, i.sku) AS sku, c.product_status, i.brand, c.child_sku, i.category1, i.category2, i.category3, i.category4, i.first_grn_date
           FROM ( SELECT upper(COALESCE(p.parent_sku, e.sku)::text) AS sku, "max"(cp.product_status::text) AS product_status, upper("max"(COALESCE(p.child_sku, e.sku)::text)) AS child_sku
                   FROM magento.catalog_product_entity e
              LEFT JOIN magento.celebrity_product cp ON e.row_id = cp.product_entity_id
         LEFT JOIN analytics.parent_child_sku_mapping p ON upper(e.sku::text) = upper(p.child_sku::text)
        GROUP BY upper(COALESCE(p.parent_sku, e.sku)::text)) c
      LEFT JOIN ( SELECT upper(nav_sku_master.sku::text) AS sku, nav_sku_master.brand, nav_sku_master.category1, nav_sku_master.category2, nav_sku_master.category3, nav_sku_master.category4, date(min(nav_sku_master.first_grn_date)) AS first_grn_date
                   FROM nav.nav_sku_master
                  GROUP BY upper(nav_sku_master.sku::text), nav_sku_master.brand, nav_sku_master.category1, nav_sku_master.category2, nav_sku_master.category3, nav_sku_master.category4) i ON c.child_sku = i.sku) c
   FULL JOIN ( SELECT COALESCE(i.sku, s.display_sku) AS sku, i.impression_7days, i.impression_14days, i.impression_28days, i.impression_90days, s.qty_7days, s.qty_14days, s.qty_28days, s.qty_90days, s.rev_7days, s.rev_14days, s.rev_28days, s.rev_90days
           FROM ( SELECT upper(COALESCE(b.parent_sku, a.sku)::text) AS display_sku, sum(
                        CASE
                            WHEN a.order_date >= ('now'::text::date - 7) AND a.order_date <= 'now'::text::date THEN a.quantity
                            ELSE 0::bigint
                        END) AS qty_7days, sum(
                        CASE
                            WHEN a.order_date >= ('now'::text::date - 14) AND a.order_date <= 'now'::text::date THEN a.quantity
                            ELSE 0::bigint
                        END) AS qty_14days, sum(
                        CASE
                            WHEN a.order_date >= ('now'::text::date - 28) AND a.order_date <= 'now'::text::date THEN a.quantity
                            ELSE 0::bigint
                        END) AS qty_28days, sum(
                        CASE
                            WHEN a.order_date >= ('now'::text::date - 90) AND a.order_date <= 'now'::text::date THEN a.quantity
                            ELSE 0::bigint
                        END) AS qty_90days, sum(
                        CASE
                            WHEN a.order_date >= ('now'::text::date - 7) AND a.order_date <= 'now'::text::date THEN a.net_sale_price_kwd
                            ELSE 0::numeric
                        END) AS rev_7days, sum(
                        CASE
                            WHEN a.order_date >= ('now'::text::date - 14) AND a.order_date <= 'now'::text::date THEN a.net_sale_price_kwd
                            ELSE 0::numeric
                        END) AS rev_14days, sum(
                        CASE
                            WHEN a.order_date >= ('now'::text::date - 28) AND a.order_date <= 'now'::text::date THEN a.net_sale_price_kwd
                            ELSE 0::numeric
                        END) AS rev_28days, sum(
                        CASE
                            WHEN a.order_date >= ('now'::text::date - 90) AND a.order_date <= 'now'::text::date THEN a.net_sale_price_kwd
                            ELSE 0::numeric
                        END) AS rev_90days
                   FROM ( SELECT order_details.order_date, upper(order_details.sku::text)::character varying AS sku, sum(order_details.quantity) AS quantity, sum(order_details.net_sale_price_kwd) AS net_sale_price_kwd
                           FROM aoi.order_details
                          WHERE order_details.order_category::text = 'NORMAL'::text AND order_details.order_date >= ('now'::text::date - 90) AND order_details.bundle_id IS NULL
                          GROUP BY order_details.order_date, upper(order_details.sku::text)::character varying
                UNION ALL 
                         SELECT order_details.order_date, upper(order_details.bundle_id::text)::character varying AS sku, count(DISTINCT concat(order_details.order_number::text, order_details.bundle_seq_id::text)) AS quantity, sum(order_details.net_sale_price_kwd) AS net_sale_price_kwd
                           FROM aoi.order_details
                          WHERE order_details.order_category::text = 'NORMAL'::text AND order_details.order_date >= ('now'::text::date - 90) AND order_details.bundle_id IS NOT NULL
                          GROUP BY order_details.order_date, upper(order_details.bundle_id::text)::character varying) a
              LEFT JOIN analytics.parent_child_sku_mapping b ON upper(a.sku::text) = upper(b.child_sku::text)
             GROUP BY upper(COALESCE(b.parent_sku, a.sku)::text)) s
      FULL JOIN ( SELECT upper(impressions_trends.sku::text) AS sku, sum(
                        CASE
                            WHEN impressions_trends.event_date >= ('now'::text::date - 7) AND impressions_trends.event_date <= 'now'::text::date THEN impressions_trends.impressions
                            ELSE 0
                        END) AS impression_7days, sum(
                        CASE
                            WHEN impressions_trends.event_date >= ('now'::text::date - 14) AND impressions_trends.event_date <= 'now'::text::date THEN impressions_trends.impressions
                            ELSE 0
                        END) AS impression_14days, sum(
                        CASE
                            WHEN impressions_trends.event_date >= ('now'::text::date - 28) AND impressions_trends.event_date <= 'now'::text::date THEN impressions_trends.impressions
                            ELSE 0
                        END) AS impression_28days, sum(
                        CASE
                            WHEN impressions_trends.event_date >= ('now'::text::date - 90) AND impressions_trends.event_date <= 'now'::text::date THEN impressions_trends.impressions
                            ELSE 0
                        END) AS impression_90days
                   FROM firebase.impressions_trends
                  WHERE impressions_trends.event_name::text = 'product_impression'::text AND impressions_trends.sku IS NOT NULL AND impressions_trends.event_date >= ('now'::text::date - 90)
                  GROUP BY upper(impressions_trends.sku::text)) i ON i.sku = s.display_sku) m ON c.sku = m.sku
   LEFT JOIN ( SELECT upper(inventory_health.sku::text) AS sku, min(inventory_health.soh) AS soh, min(date(
           CASE
               WHEN inventory_health.first_order_date::text = 'No_sale'::text THEN NULL::character varying
               ELSE inventory_health.first_order_date
           END::text)) AS first_sell_date, min(date(inventory_health.first_grn_date)) AS first_grn_date, min(inventory_health.open_po_qty) AS open_po_qty, min(inventory_health.stock_cover_60days_including_open_po::text) AS stock_cover, min(inventory_health.stock_cover_flag_including_open_po::text) AS open_po_flag
      FROM aoi.inventory_health
     GROUP BY upper(inventory_health.sku::text)) q ON c.child_sku = q.sku
   LEFT JOIN ( SELECT upper(COALESCE(magento_sku_master.config_sku, magento_sku_master.sku)::text) AS sku, min(date(magento_sku_master.enable_date)) AS enable_date
   FROM magento.magento_sku_master
  WHERE magento_sku_master.enable_date IS NOT NULL
  GROUP BY upper(COALESCE(magento_sku_master.config_sku, magento_sku_master.sku)::text)) d ON c.sku = d.sku
  WITH NO SCHEMA BINDING;