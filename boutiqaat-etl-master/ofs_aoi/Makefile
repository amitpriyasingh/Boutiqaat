SHELL := /bin/bash
MYSQL = mysql --defaults-file=/credentials/.mylogin.cnf


aoi_order_details: copy_credentials sync_order_details 

aoi_order_details_update: copy_credentials update_order_details

copy_credentials:
	cp /credentials/my.cnf /etc/my.cnf

sync_order_details:
	mysql < queries/sync_order_details.sql

update_order_details:
	mysql < queries/sync_order_details_update.sql