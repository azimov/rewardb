SELECT fr.TARGET_COHORT_ID, t.TARGET_COHORT_NAME, fr.OUTCOME_COHORT_ID, o.OUTCOME_COHORT_NAME, ec.ATC3_CONCEPT_NAME AS EXPOSURE_CLASS,
    CASE
        WHEN harm_t.THRESH_COUNT IS NULL THEN 'none'
        WHEN harm_t.THRESH_COUNT = 1 THEN 'one'
        WHEN harm_t.THRESH_COUNT >= 4 THEN 'all'
        WHEN harm_t.THRESH_COUNT > 1 THEN 'most'
        ELSE harm_t.THRESH_COUNT
    END AS sc_risk,
    CASE
        WHEN benefit_t.THRESH_COUNT IS NULL THEN 'none'
        WHEN benefit_t.THRESH_COUNT = 1 THEN 'one'
        WHEN benefit_t.THRESH_COUNT >= 4 THEN 'all'
        WHEN benefit_t.THRESH_COUNT > 1 THEN 'most'
        ELSE benefit_t.THRESH_COUNT
    END AS sc_benefit
FROM results fr
    
    LEFT JOIN @riskTable harm_t ON harm_t.TARGET_COHORT_ID = fr.TARGET_COHORT_ID AND harm_t.OUTCOME_COHORT_ID = fr.OUTCOME_COHORT_ID
    LEFT JOIN @benefitTable benefit_t ON benefit_t.TARGET_COHORT_ID = fr.TARGET_COHORT_ID AND benefit_t.OUTCOME_COHORT_ID = fr.OUTCOME_COHORT_ID
    
    INNER JOIN target t ON t.target_cohort_id = fr.target_cohort_id
    INNER JOIN target_concept tc ON tc.target_cohort_id = t.target_cohort_id
    INNER JOIN outcome o ON o.outcome_cohort_id = fr.outcome_cohort_id
    LEFT JOIN exposure_classes ec ON ec.CONCEPT_ID = tc.TARGET_CONCEPT_ID
    
GROUP BY fr.TARGET_COHORT_ID, fr.OUTCOME_COHORT_ID