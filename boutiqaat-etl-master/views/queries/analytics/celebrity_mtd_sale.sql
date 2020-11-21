CREATE OR REPLACE VIEW analytics.celebrity_mtd_sale AS
SELECT cmst.target_month, cmst.celebrity_id, cmst.celebrity_name, cmst.code, cmst.celeb_mtd_sale, cmst.celeb_mtd_target, cmst.celeb_total_target, cmst.account_manager, cmst.am_email, cmst.rm_email, cmst.am_celeb_count, CASE WHEN (cmst.celeb_mtd_sale >= cmst.celeb_mtd_target) THEN 1 ELSE 0 END AS celeb_proj_achv_flag, CASE WHEN (cmst.celeb_mtd_sale >= cmst.celeb_total_target) THEN 1 ELSE 0 END AS celeb_mtd_achv_flag, CASE WHEN ((cmst.rm_email)::text = 'd.ashour@boutiqaat.com'::text) THEN 1 ELSE 0 END AS dubai_team_flag, (1 / CASE WHEN (cmst.am_celeb_count = NULL::bigint) THEN NULL::bigint ELSE cmst.am_celeb_count END) AS celeb_comm_factor, CASE WHEN ((cmst.celeb_mtd_sale >= cmst.celeb_mtd_target) AND ((cmst.rm_email)::text <> 'd.ashour@boutiqaat.com'::text)) THEN (200 * (1 / CASE WHEN (cmst.am_celeb_count = NULL::bigint) THEN NULL::bigint ELSE cmst.am_celeb_count END)) ELSE (0)::bigint END AS celeb_proj_comm_kwd, CASE WHEN ((cmst.celeb_mtd_sale >= cmst.celeb_total_target) AND ((cmst.rm_email)::text <> 'd.ashour@boutiqaat.com'::text)) THEN (200 * (1 / CASE WHEN (cmst.am_celeb_count = NULL::bigint) THEN NULL::bigint ELSE cmst.am_celeb_count END)) ELSE (0)::bigint END AS celeb_mtd_comm_kwd, CASE WHEN ((cmst.celeb_mtd_sale >= cmst.celeb_mtd_target) AND ((cmst.rm_email)::text = 'd.ashour@boutiqaat.com'::text)) THEN (3000 * (1 / CASE WHEN (cmst.am_celeb_count = NULL::bigint) THEN NULL::bigint ELSE cmst.am_celeb_count END)) ELSE (0)::bigint END AS celeb_proj_comm_aed, CASE WHEN ((cmst.celeb_mtd_sale >= cmst.celeb_total_target) AND ((cmst.rm_email)::text = 'd.ashour@boutiqaat.com'::text)) THEN (3000 * (1 / CASE WHEN (cmst.am_celeb_count = NULL::bigint) THEN NULL::bigint ELSE cmst.am_celeb_count END)) ELSE (0)::bigint END AS celeb_mtd_comm_aed FROM aoi.celebrity_mtd_sale_tab cmst
WITH NO SCHEMA BINDING;