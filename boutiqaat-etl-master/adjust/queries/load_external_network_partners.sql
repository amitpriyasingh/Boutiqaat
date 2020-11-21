{% set PATHYEAR = "{}".format(PARTPATH[51:55]) %}
{% set PATHMONTH = "{}".format(PARTPATH[71:73]) %}
{% set PATHDATE = "{}".format(PARTPATH[88:98]) %}
{% set INGDATE = "{}".format(PARTPATH[114:124]) %}

alter table spectrum.network_partners
add if not exists partition (tracking_year='{{PATHYEAR}}', tracking_month='{{PATHMONTH}}', tracking_date='{{PATHDATE}}', ingestion_date='{{INGDATE}}')
location '{{PARTPATH}}';