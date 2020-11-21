BEGIN;

DROP TABLE IF EXISTS analytics.customer_repeat_rate;

SELECT * INTO analytics.customer_repeat_rate
FROM
(
SELECT
       EXTRACT(month from acquisition_date) acquisition_month,
       EXTRACT(year from acquisition_date) acquisition_year,
       SUM(m30) sum_m30,
       SUM(m60) sum_m60,
       SUM(m90) sum_m90,
       COUNT(m30) count_m30,
       COUNT(m60) count_m60,
       COUNT(m90) count_m90
FROM
(select
       phone_no,
       acquisition_date,
       gap_btw_1st_2nd_trans,
       CASE WHEN gap_btw_1st_2nd_trans between 1 and 30 THEN 1 ELSE 0 END m30,
       CASE WHEN gap_btw_1st_2nd_trans between 1 and 60 THEN 1 ELSE 0 END m60,
       CASE WHEN gap_btw_1st_2nd_trans between 1 and 90 THEN 1 ELSE 0 END m90
from analytics.customer_retention)cr
Group By acquisition_month,acquisition_year
);

COMMIT;