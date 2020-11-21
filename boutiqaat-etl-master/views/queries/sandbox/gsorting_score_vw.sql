CREATE OR REPLACE VIEW sandbox.gsorting_score_vw AS
SELECT gsorting_score.sku, gsorting_score.brand, gsorting_score.category1, gsorting_score.category2, gsorting_score.global_score, gsorting_score.score_rank FROM sandbox.gsorting_score WHERE (gsorting_score.celebrity_id = 0)
WITH NO SCHEMA BINDING;