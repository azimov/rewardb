if object_id('@results_database_schema.@merged_results_table', 'U') is not null
	drop table @results_database_schema.@merged_results_table
;

create table @results_database_schema.@merged_results_table
(
  analysis_id int
  , source_id int
  , target_cohort_id bigint
  , outcome_cohort_id bigint
  , t_at_risk int
  , t_pt float
  , t_cases int
  , c_at_risk int
  , c_cases int
  , c_pt float
  , relative_risk float
  , lb_95 float
  , ub_95 float
  , log_rr float
  , se_log_rr float
  , p_value float
)
with (distribution = replicate);
