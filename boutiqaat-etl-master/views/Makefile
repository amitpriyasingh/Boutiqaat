SHELL := /bin/bash
# S3CMD=aws s3

# daily: analytics sandbox


# ####### Analytics #######

# analytics: events_report high_cov

# high_cov: high_cover high_cover_beauty high_cover_supp_fit high_cover_trdlwear high_cover_accessories high_cover_orientalfragrances high_cover_sports_and_fashion high_cover_pl_team

# high_cover:
# 	cat queries/analytics/high_cover.sql | redshift

# high_cover_beauty:
# 	cat queries/analytics/high_cover_beauty.sql | redshift

# high_cover_supp_fit:
# 	cat queries/analytics/high_cover_supp_fit.sql | redshift

# high_cover_trdlwear:
# 	cat queries/analytics/high_cover_trdlwear.sql | redshift

# high_cover_accessories:
# 	cat queries/analytics/high_cover_accessories.sql | redshift

# high_cover_orientalfragrances:
# 	cat queries/analytics/high_cover_orientalfragrances.sql | redshift

# high_cover_sports_and_fashion:
# 	cat queries/analytics/high_cover_sports_and_fashion.sql | redshift

# high_cover_pl_team:
# 	cat queries/analytics/high_cover_pl_team.sql | redshift

# soh_report:
# 	cat queries/analytics/soh_report.sql | redshift

# events_report:
# 	cat queries/analytics/events_report.sql | redshift


# ####### Sandbox #######

# sandbox: cust_gap_trans

# cust_gap_trans: cust_gap_1st_2nd_trans cust_gap_2nd_3rd_trans cust_gap_3rd_4th_trans cust_gap_4th_5th_trans soh_report

# cust_gap_1st_2nd_trans:
# 	cat queries/sandbox/cust_gap_1st_2nd_trans.sql | redshift

# cust_gap_2nd_3rd_trans:
# 	cat queries/sandbox/cust_gap_2nd_3rd_trans.sql | redshift

# cust_gap_3rd_4th_trans:
# 	cat queries/sandbox/cust_gap_3rd_4th_trans.sql | redshift

# cust_gap_4th_5th_trans:
# 	cat queries/sandbox/cust_gap_4th_5th_trans.sql | redshift

# gsku_celebrity_impression_stock_vw:
# 	cat queries/sandbox/gsku_celebrity_impression_stock_vw.sql | redshift

# sku_celebrity_impression_stock_vw:
# 	cat queries/sandbox/sku_celebrity_impression_stock_vw.sql | redshift
# 
#    
	

# ####### OFS #######

# ofs: celebrity_am_mapping

# celebrity_am_mapping:
# 	cat queries/ofs/celebrity_am_mapping.sql | redshift

VIEWDIR = .
sync_views: $(VIEWDIR)/queries/*/*
	for file in $^ ; do \
		cat $${file} | redshift ; \
	done
