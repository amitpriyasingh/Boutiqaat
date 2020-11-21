CREATE OR REPLACE VIEW sandbox.marketing_cancellation_per_channel AS
SELECT tx_attr_7days_201920_extended_tagged.order_date, tx_attr_7days_201920_extended_tagged.adj_network_name, tx_attr_7days_201920_extended_tagged.adj_match_type, sum(tx_attr_7days_201920_extended_tagged.bi_revenue_usd) AS total_revenue, sum(CASE WHEN ((tx_attr_7days_201920_extended_tagged.order_status_flag)::text = ('Full_cancelled'::character varying)::text) THEN tx_attr_7days_201920_extended_tagged.bi_revenue_usd ELSE ((0)::numeric)::numeric(18,0) END) AS full_cancelled_revenue, sum(CASE WHEN ((tx_attr_7days_201920_extended_tagged.order_status_flag)::text = ('Partial_cancelled'::character varying)::text) THEN tx_attr_7days_201920_extended_tagged.bi_revenue_usd ELSE ((0)::numeric)::numeric(18,0) END) AS partial_cancelled_revenue, sum(CASE WHEN ((tx_attr_7days_201920_extended_tagged.order_status_flag)::text = ('Not_Cancelled'::character varying)::text) THEN tx_attr_7days_201920_extended_tagged.bi_revenue_usd ELSE ((0)::numeric)::numeric(18,0) END) AS not_cancelled_revenue, sum(CASE WHEN ((tx_attr_7days_201920_extended_tagged.order_status_flag)::text = ('Not_found_in_OFS'::character varying)::text) THEN tx_attr_7days_201920_extended_tagged.bi_revenue_usd ELSE ((0)::numeric)::numeric(18,0) END) AS not_found_in_ofs_revenue, (sum(CASE WHEN ((tx_attr_7days_201920_extended_tagged.order_status_flag)::text = ('Full_cancelled'::character varying)::text) THEN tx_attr_7days_201920_extended_tagged.bi_revenue_usd ELSE ((0)::numeric)::numeric(18,0) END) / sum(tx_attr_7days_201920_extended_tagged.bi_revenue_usd)) AS full_cancelled_per, (sum(CASE WHEN ((tx_attr_7days_201920_extended_tagged.order_status_flag)::text = ('Partial_cancelled'::character varying)::text) THEN tx_attr_7days_201920_extended_tagged.bi_revenue_usd ELSE ((0)::numeric)::numeric(18,0) END) / sum(tx_attr_7days_201920_extended_tagged.bi_revenue_usd)) AS partial_cancelled_per, (sum(CASE WHEN ((tx_attr_7days_201920_extended_tagged.order_status_flag)::text = ('Not_Cancelled'::character varying)::text) THEN tx_attr_7days_201920_extended_tagged.bi_revenue_usd ELSE ((0)::numeric)::numeric(18,0) END) / sum(tx_attr_7days_201920_extended_tagged.bi_revenue_usd)) AS not_cancelled_per, (sum(CASE WHEN ((tx_attr_7days_201920_extended_tagged.order_status_flag)::text = ('Not_found_in_OFS'::character varying)::text) THEN tx_attr_7days_201920_extended_tagged.bi_revenue_usd ELSE ((0)::numeric)::numeric(18,0) END) / sum(tx_attr_7days_201920_extended_tagged.bi_revenue_usd)) AS not_found_in_ofs_per FROM sandbox.tx_attr_7days_201920_extended_tagged GROUP BY tx_attr_7days_201920_extended_tagged.order_date, tx_attr_7days_201920_extended_tagged.adj_network_name, tx_attr_7days_201920_extended_tagged.adj_match_type
WITH NO SCHEMA BINDING;