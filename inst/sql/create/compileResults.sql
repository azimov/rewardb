insert into @results_database_schema.@merged_results_table
(
  analysis_id
  , source_id
  , target_cohort_id
  , outcome_cohort_id
  , t_at_risk
  , t_pt
  , t_cases
  , c_at_risk
  , c_cases
  , c_pt
  , relative_risk
  , lb_95
  , ub_95
  , log_rr
  , se_log_rr
  , p_value
)
select
  1 as analysis_id
  , @source_id as source_id
  , exposureId as target_cohort_id
  , outcomeId as outcome_cohort_id
  , numPersons as t_at_risk
  , timeAtRiskExposed as t_pt
  , numOutcomesExposed as t_cases
  , numPersons as c_at_risk
  , numOutcomesUnexposed as c_cases
  , timeAtRiskUnexposed as c_pt
  , irr as relative_risk
  , irrLb95 as lb_95
  , irrUb95 as ub_95
  , logRr as log_rr
  , seLogRr as se_log_rr
  , p as p_value
from @results_database_schema.@results_table
{@custom_cohorts_only} ? {WHERE outcomeId IN (@custom_outcome_cohorts)}
;