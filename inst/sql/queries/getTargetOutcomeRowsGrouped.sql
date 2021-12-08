SELECT r.SOURCE_ID,
    ds.SOURCE_NAME,
    r.RR,
    CONCAT(ROUND(r.LB_95, 2), '-', ROUND(r.UB_95, 2)) AS CI_95,
    r.LB_95,
    r.UB_95,
    r.P_VALUE,
    r2.RR as Calibrated_RR,
    CONCAT(ROUND(r2.LB_95, 2) , '-', ROUND(r2.UB_95, 2)) AS CALIBRATED_CI_95,
    r2.LB_95 as CALIBRATED_LB_95,
    r2.UB_95 as CALIBRATED_UB_95,
    r2.P_VALUE as Calibrated_P_VALUE,
    r.C_AT_RISK,
    r.C_PT,
    r.C_CASES,
    r.T_AT_RISK,
    r.T_PT,
    r.T_CASES
FROM @schema.result r
INNER JOIN @schema.data_source ds ON ds.source_id = r.source_id
LEFT JOIN @schema.result r2 ON (
        r2.OUTCOME_COHORT_ID = r.OUTCOME_COHORT_ID
        AND r2.TARGET_COHORT_ID = r.TARGET_COHORT_ID
        AND r2.calibrated = 1
        AND r2.source_id = r.source_id
    )
    WHERE r.OUTCOME_COHORT_ID = @outcome
    AND r.TARGET_COHORT_ID = @treatment
    AND r.calibrated = 0
ORDER BY r.SOURCE_ID