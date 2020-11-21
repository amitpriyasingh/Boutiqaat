SHELL := /bin/bash
include /credentials/var.env
export
MSSQLCLI = navcli


master_sync:
	$(MSSQLCLI) -e -i query.sql
	$(MAKE) -C ../magento magento_sku_master
	$(MAKE) -C ../NAV nav_soh
	$(MAKE) -C ../redshift unload_soh_report

nav_master_sync:
	$(MSSQLCLI) -e -i query.sql