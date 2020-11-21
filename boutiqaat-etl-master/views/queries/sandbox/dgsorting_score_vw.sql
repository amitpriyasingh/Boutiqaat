CREATE OR REPLACE VIEW sandbox.dgsorting_score_vw AS
SELECT dgsorting_score.sku, dgsorting_score.brand, dgsorting_score.category1, dgsorting_score.category2, dgsorting_score.dglobal_score, pg_catalog.row_number() OVER(  ORDER BY dgsorting_score.dglobal_score) AS dgscore_rank FROM sandbox.dgsorting_score WHERE (dgsorting_score.celebrity_id = 0)
WITH NO SCHEMA BINDING;