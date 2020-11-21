{% set PATHYEAR = "{}".format(PARTPATH[42:46]) %}
{% set PATHMONTH = "{}".format(PARTPATH[62:64]) %}
{% set PATHDATE = "{}".format(PARTPATH[79:89]) %}
{% set INGDATE = "{}".format(PARTPATH[105:115]) %}

alter table spectrum.adjust_raw_data
add if not exists partition (tracking_year='{{PATHYEAR}}', tracking_month='{{PATHMONTH}}', tracking_date='{{PATHDATE}}', ingestion_date='{{INGDATE}}')
location '{{PARTPATH}}';
