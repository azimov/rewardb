SELECT
  r.SOURCE_ID,
  ds.SOURCE_NAME,
  r.outcome_cohort_id,
  o.cohort_name as outcome_cohort_name,
  r.target_cohort_id,
  t.cohort_name as target_cohort_name,
  t.is_atc_4,
  r.RR,
  r.lb_95,
  r.ub_95,
  r.P_VALUE,
  r2.I2,
  {@show_exposure_classes}?{ STRING_AGG(distinct ec.EXPOSURE_CLASS_NAME, ';') as ATC_3_CLASS, }
  r.calibrated,
  r.T_AT_RISK as n_exposed,
  r.T_PT as exposed_time,
  r.C_CASES as unexposed_cases,
  r.T_CASES as exposed_cases
FROM @schema.result r
INNER JOIN @schema.data_source ds ON ds.source_id = r.source_id
INNER JOIN @schema.outcome o ON o.outcome_cohort_id = r.outcome_cohort_id
INNER JOIN @schema.target t ON t.target_cohort_id = r.target_cohort_id
INNER JOIN @schema.result r2 ON (r2.source_id = -99 AND r2.calibrated = 0 AND r.outcome_cohort_id = r2.outcome_cohort_id AND r2.target_cohort_id = r.target_cohort_id)
{@show_exposure_classes}?{
  INNER JOIN @schema.target_exposure_class tec ON tec.target_cohort_id = t.target_cohort_id
  INNER JOIN @schema.exposure_class ec ON ec.exposure_class_id = tec.exposure_class_id
}
WHERE r.calibrated IN (@calibrated)
GROUP BY
    r.source_id,
    ds.SOURCE_NAME,
    r.outcome_cohort_id,
    o.cohort_name,
    r.target_cohort_id,
    t.cohort_name,
    t.is_atc_4,
    r.RR,
    r.lb_95,
    r.ub_95,
    r.P_VALUE,
    r2.I2,
    r.calibrated,
    r.T_AT_RISK,
    r.T_PT,
    r.C_CASES,
    r.T_CASES