WITH benefit_t AS(
    SELECT TARGET_COHORT_ID, OUTCOME_COHORT_ID, COUNT(DISTINCT(SOURCE_ID)) AS THRESH_COUNT
    FROM result
    WHERE RR <= @benefit
        AND P_VALUE < 0.05
    GROUP BY TARGET_COHORT_ID, OUTCOME_COHORT_ID
), harm_t AS (
    SELECT TARGET_COHORT_ID, OUTCOME_COHORT_ID, COUNT(DISTINCT(SOURCE_ID)) AS THRESH_COUNT
    FROM result
    WHERE RR >= @risk
        AND P_VALUE < 0.05
    GROUP BY TARGET_COHORT_ID, OUTCOME_COHORT_ID
)

SELECT DISTINCT fr.TARGET_COHORT_ID, t.COHORT_NAME as TARGET_COHORT_NAME, fr.OUTCOME_COHORT_ID, o.COHORT_NAME AS OUTCOME_COHORT_NAME,
    CASE
        WHEN harm_t.THRESH_COUNT IS NULL THEN 'none'
        WHEN harm_t.THRESH_COUNT = 1 THEN 'one'
        WHEN harm_t.THRESH_COUNT >= 4 THEN 'all'
        WHEN harm_t.THRESH_COUNT > 1 THEN 'most'
        ELSE 'none'
    END AS sc_risk,
    CASE
        WHEN benefit_t.THRESH_COUNT IS NULL THEN 'none'
        WHEN benefit_t.THRESH_COUNT = 1 THEN 'one'
        WHEN benefit_t.THRESH_COUNT >= 4 THEN 'all'
        WHEN benefit_t.THRESH_COUNT > 1 THEN 'most'
        ELSE 'none'
    END AS sc_benefit
FROM result fr
    
    LEFT JOIN benefit_t harm_t ON harm_t.TARGET_COHORT_ID = fr.TARGET_COHORT_ID AND harm_t.OUTCOME_COHORT_ID = fr.OUTCOME_COHORT_ID
    LEFT JOIN harm_t benefit_t ON benefit_t.TARGET_COHORT_ID = fr.TARGET_COHORT_ID AND benefit_t.OUTCOME_COHORT_ID = fr.OUTCOME_COHORT_ID
    
    INNER JOIN target t ON t.target_cohort_id = fr.target_cohort_id
    INNER JOIN outcome o ON o.outcome_cohort_id = fr.outcome_cohort_id
    --LEFT JOIN exposure_classes ec ON ec.CONCEPT_ID = tc.TARGET_CONCEPT_ID