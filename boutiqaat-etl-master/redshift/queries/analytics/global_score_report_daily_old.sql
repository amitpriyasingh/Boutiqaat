BEGIN;

delete from sandbox.dgsorting_score_old 
where (select max(orderdate) from sandbox.sorting_score_alert) < current_date;

INSERT INTO sandbox.dgsorting_score_old select * from sandbox.dgsorting_score
where (select max(orderdate) from sandbox.sorting_score_alert) < current_date;

DELETE FROM sandbox.gsorting_score WHERE 1=1;
INSERT INTO sandbox.gsorting_score
select n.* , 
	   row_number() over( order by global_score desc) as score_rank
from(
select sku,celebrity_id, product_status, enable_date,first_sell_date,first_grn_date,brand,category1,category2,category3,category4,impression_7days,impression_14days,impression_28days,impression_90days,qty_7days,qty_14days,qty_28days,qty_90days,rev_7days,rev_14days,rev_28days,rev_90days,soh,open_po_qty,stock_cover,open_po_flag,last28dayrpmi,last7dayrpmi,is_new_arrival,index_fact,lavg_cc12b,lavg_cc1b,lavg_cc1,lavg_c,lavg_c12b,lavg_c1b,lavg_c1,avg_considered,score_a,score_b,score_c,score_d,score_f,
case when sorting_score=0 and impression_28days<>0 then 1/(10^6*impression_28days) else sorting_score end as global_score
from (
select d.*, 
	CASE WHEN is_new_arrival = 1 THEN GREATEST(score_A*score_C , score_D*score_F) 
         ELSE CASE WHEN last28dayrpmi IS NULL THEN score_D/NULLIF(score_F,0)  ELSE score_A*score_C END
	END as sorting_score
from
(select c.*, 
	  last28dayrpmi as score_A, 
	  index_fact as score_B, 
	 case when index_fact < 1.0 then 1.0 when index_fact between 1.0 and 2.0 then (index_fact +1.0)/2.0 else 1.0 end as score_C, 
	 avg_considered as score_D,
	 (random() * 0.04 + 1.230) as score_F
 from 
	( select a.*, 
		 1.00000000 *rev_28days/nullif(impression_28days,0) as last28dayrpmi ,
		 1.00000000*rev_7days/nullif(impression_7days,0) as last7dayrpmi ,
		 case when coalesce(enable_date, first_grn_date, first_sell_date) > current_date - 14 then 1 else 0 end is_new_arrival,
		 1.00000000*(rev_7days/nullif(impression_7days,0))/nullif((rev_28days/nullif(impression_28days,0)),0) as index_fact,
		 lavg_cc12b, lavg_cc1b ,lavg_cc1, lavg_c ,lavg_c12b ,lavg_c1b ,lavg_c1,
		 coalesce(nullif(lavg_cc12b,0.0000), nullif(lavg_cc1b,0.0000) ,nullif(lavg_cc1,0.0000), nullif(lavg_c,0.0000),nullif(lavg_c12b,0.0000),nullif(lavg_c1b,0.0000),nullif(lavg_c1,0.0000), 0) as avg_considered
	from sandbox.gsku_celebrity_impression_stock_vw a
	left join (
				select sku, celebrity_id, category1, category2, brand,
				1.00000000*(sum(rev_28days) over (partition by category1, category2, brand) )/nullif((sum(impression_28days) over(partition by  category1, category2, brand) ) ,0) as  lavg_cc12b, 
				1.00000000*(sum(rev_28days) over(partition by category1, brand) )/nullif((sum(impression_28days) over(partition by category1, brand) ) ,0) as  lavg_cc1b,
				1.00000000*(sum(rev_28days) over(partition by category1) )/nullif((sum(impression_28days) over(partition by category1) ) ,0) as  lavg_cc1,
				1.00000000*(sum(rev_28days) over())/nullif((sum(impression_28days) over() ) ,0) as  lavg_c,
				1.00000000*(sum(rev_28days) over(partition by category1, category2, brand))/nullif((sum(impression_28days) over(partition by category1, category2, brand) ) ,0) as lavg_c12b,
				1.00000000*(sum(rev_28days) over(partition by category1, brand) )/nullif((sum(impression_28days) over(partition by category1, brand)),0 ) as lavg_c1b,
				1.00000000*(sum(rev_28days) over(partition by category1) )/nullif((sum(impression_28days) over(partition by category1)) ,0) as lavg_c1
				from sandbox.gsku_celebrity_impression_stock_vw
			  ) b on a.sku = b.sku and a.celebrity_id = b.celebrity_id 
	) c
) d 
) m ) n  ;	

DELETE FROM  sandbox.dgsorting_score WHERE 1=1;
INSERT INTO sandbox.dgsorting_score
select b.*, 
	   row_number() over( partition by celebrity_id order by b.dglobal_score desc) as dgscore_rank
from(
select g.*, avg_impression, 
	   case when (score_rank < P1rank and impression_28days < avg_impression*0.1) then global_score*0.8 else global_score end as dglobal_score
from sandbox.gsorting_score as g ,
(select 1.00000000*sum(impression_28days)/count(sku) avg_impression, 0.01*max(score_rank) P1rank from sandbox.gsorting_score) a
 ) b ;

delete from sandbox.sorting_score_alert where orderdate = current_date;

INSERT INTO sandbox.sorting_score_alert 
select current_date as orderdate, count(sku) as total_sku,
count(case when impression_7days = 0 then sku end) as sku_no_imp_7days,
count(case when impression_14days = 0 then sku end) as sku_no_imp_14days,
count(case when impression_28days = 0 then sku end) as sku_no_imp_28days,
count(case when rev_7days = 0 then sku end) as sku_no_rev_7days,
count(case when rev_14days = 0 then sku end) as sku_no_rev_14days,
count(case when rev_28days = 0 then sku end) as sku_no_rev_28days,
count(case when is_new_arrival = 1 then sku end) as new_sku,
count(case when is_new_arrival = 1 and dglobal_score = 0 then sku end) as new_sku_no_score,
count(case when is_new_arrival = 0 and impression_28days = 0 then sku end) as old_sku_no_imp_28days,
count(case when dglobal_score = 0 then sku end) as total_sku_no_score
from  sandbox.dgsorting_score;  

DELETE FROM sandbox.sorting_score WHERE 1=1;
INSERT INTO sandbox.sorting_score
select n.* , 
	   row_number() over( partition by celebrity_id order by sorting_score desc) as score_rank
from(
select sku,celebrity_id, product_status, enable_date,first_sell_date,first_grn_date,brand,category1,category2,category3,category4,impression_7days,impression_14days,impression_28days,impression_90days,qty_7days,qty_14days,qty_28days,qty_90days,rev_7days,rev_14days,rev_28days,rev_90days,soh,open_po_qty,stock_cover,open_po_flag,last28dayrpmi,last7dayrpmi,is_new_arrival,index_fact,lavg_cc12b,lavg_cc1b,lavg_cc1,lavg_c,lavg_c12b,lavg_c1b,lavg_c1,avg_considered,score_a,score_b,score_c,score_d,score_f,
case when sorting_score=0 and impression_28days<>0 then 1/(10^6*impression_28days) else sorting_score end as sorting_score
from (
select d.*, 
	CASE WHEN is_new_arrival = 1 THEN GREATEST(score_A*score_C , score_D*score_F) 
         ELSE CASE WHEN last28dayrpmi IS NULL THEN score_D/NULLIF(score_F,0)  ELSE score_A*score_C END
	END as sorting_score
from
(select c.*, 
	  last28dayrpmi as score_A, 
	  index_fact as score_B, 
	 case when index_fact < 1.0 then 1.0 when index_fact between 1.0 and 2.0 then (index_fact +1.0)/2.0 else 1.0 end as score_C, 
	 avg_considered as score_D,
	 (random() * 0.04 + 1.230) as score_F
 from 
	( select a.*, 
		 1.00000000 *rev_28days/nullif(impression_28days,0) as last28dayrpmi ,
		 1.00000000*rev_7days/nullif(impression_7days,0) as last7dayrpmi ,
		 case when coalesce(enable_date, first_grn_date, first_sell_date) > current_date - 14 then 1 else 0 end is_new_arrival,
		 1.00000000*(rev_7days/nullif(impression_7days,0))/nullif((rev_28days/nullif(impression_28days,0)),0) as index_fact,
		 lavg_cc12b, lavg_cc1b ,lavg_cc1, lavg_c ,lavg_c12b ,lavg_c1b ,lavg_c1,
		 coalesce(nullif(lavg_cc12b,0.0000), nullif(lavg_cc1b,0.0000) ,nullif(lavg_cc1,0.0000), nullif(lavg_c,0.0000),nullif(lavg_c12b,0.0000),nullif(lavg_c1b,0.0000),nullif(lavg_c1,0.0000), 0) as avg_considered
	from sandbox.sku_celebrity_impression_stock_vw a
	left join (
				select sku, celebrity_id, category1, category2, brand,
				1.00000000*(sum(rev_28days) over (partition by celebrity_id, category1, category2, brand) )/nullif((sum(impression_28days) over(partition by celebrity_id, category1, category2, brand) ) ,0) as  lavg_cc12b, 
				1.00000000*(sum(rev_28days) over(partition by celebrity_id, category1, brand) )/nullif((sum(impression_28days) over(partition by celebrity_id, category1, brand) ) ,0) as  lavg_cc1b,
				1.00000000*(sum(rev_28days) over(partition by celebrity_id, category1) )/nullif((sum(impression_28days) over(partition by celebrity_id, category1) ) ,0) as  lavg_cc1,
				1.00000000*(sum(rev_28days) over(partition by celebrity_id))/nullif((sum(impression_28days) over(partition by celebrity_id) ) ,0) as  lavg_c,
				1.00000000*(sum(rev_28days) over(partition by category1, category2, brand))/nullif((sum(impression_28days) over(partition by category1, category2, brand) ) ,0) as lavg_c12b,
				1.00000000*(sum(rev_28days) over(partition by category1, brand) )/nullif((sum(impression_28days) over(partition by category1, brand)),0 ) as lavg_c1b,
				1.00000000*(sum(rev_28days) over(partition by category1) )/nullif((sum(impression_28days) over(partition by category1)) ,0) as lavg_c1
				from sandbox.sku_celebrity_impression_stock_vw
			  ) b on a.sku = b.sku and a.celebrity_id = b.celebrity_id 
	) c
) d 
) m ) n  ;	
COMMIT;