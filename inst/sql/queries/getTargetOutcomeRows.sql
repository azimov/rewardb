{DEFAULT @result = 'result'}
{DEFAULT @use_calibration = TRUE}
SELECT r.SOURCE_ID,
    ds.SOURCE_NAME,
    r.C_AT_RISK,
    r.C_PT,
    r.C_CASES,
    r.RR,
    r.LB_95,
    r.UB_95,
    r.P_VALUE,
    r.T_AT_RISK,
    r.T_PT,
    r.T_CASES,
    r.SE_LOG_RR,
    {@use_calibration} ? { r.calibrated, }
    r.I2
FROM @schema.@result r
INNER JOIN @schema.data_source ds ON ds.source_id = r.source_id
    WHERE r.OUTCOME_COHORT_ID = @outcome
    AND r.TARGET_COHORT_ID = @treatment
    {@use_calibration} ? { AND r.calibrated IN (@calibrated) }
ORDER BY r.SOURCE_ID