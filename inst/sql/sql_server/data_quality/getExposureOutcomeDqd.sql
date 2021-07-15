SELECT cd.cohort_definition_name as exposure_cohort,
       ocd.cohort_definition_name as outcome_cohort,
       min(r.RR) as min_rr,
       max(r.RR) as max_rr,
       STRING_AGG(distinct ds.source_key , ', ') as sources_present,
       count(r.source_id) as num_data_sources,
       min(r.P_VALUE) as min_p_value,
       max(r.P_VALUE) as max_p_value,
       min(r.C_CASES) as min_c_cases,
       max(r.C_CASES) as max_c_cases,
       min(r.T_AT_RISK) as min_t_at_risk,
       max(r.T_AT_RISK) as max_t_at_risk,
       min(r.T_CASES) as min_t_cases,
       max(r.T_CASES) as max_t_cases
  FROM @schema.scc_result r
  INNER JOIN @schema.data_source ds ON ds.source_id = r.source_id
  INNER JOIN @schema.outcome_cohort_definition ocd on r.outcome_cohort_id = ocd.cohort_definition_id
  INNER JOIN @schema.cohort_definition cd on r.target_cohort_id = cd.cohort_definition_id
  WHERE r.analysis_id = @analysis_id
  AND r.source_id != -99
  AND ( @inner_query )

  group by cd.cohort_definition_name, ocd.cohort_definition_name, cd.cohort_definition_id, ocd.cohort_definition_id