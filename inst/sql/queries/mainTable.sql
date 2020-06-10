WITH benefit_t AS(
    SELECT TARGET_COHORT_ID, OUTCOME_COHORT_ID, COUNT(DISTINCT(SOURCE_ID)) AS THRESH_COUNT
    FROM @schema.result
    WHERE RR <= @benefit
        AND P_VALUE < 0.05
    GROUP BY TARGET_COHORT_ID, OUTCOME_COHORT_ID
),

risk_t AS (
    SELECT TARGET_COHORT_ID, OUTCOME_COHORT_ID, COUNT(DISTINCT(SOURCE_ID)) AS THRESH_COUNT
    FROM @schema.result
    WHERE RR >= @risk
        AND P_VALUE < 0.05
    GROUP BY TARGET_COHORT_ID, OUTCOME_COHORT_ID
)

SELECT
    DISTINCT fr.TARGET_COHORT_ID, t.COHORT_NAME as TARGET_COHORT_NAME,
    fr.OUTCOME_COHORT_ID, o.COHORT_NAME AS OUTCOME_COHORT_NAME,
    CASE
        WHEN risk_t.THRESH_COUNT IS NULL THEN 'none'
        WHEN risk_t.THRESH_COUNT = 1 THEN 'one'
        WHEN risk_t.THRESH_COUNT >= 4 THEN 'all'
        WHEN risk_t.THRESH_COUNT > 1 THEN 'most'
    END AS risk_count,
    CASE
        WHEN benefit_t.THRESH_COUNT IS NULL THEN 'none'
        WHEN benefit_t.THRESH_COUNT = 1 THEN 'one'
        WHEN benefit_t.THRESH_COUNT >= 4 THEN 'all'
        WHEN benefit_t.THRESH_COUNT > 1 THEN 'most'
    END AS benefit_count
FROM @schema.result fr
    
    LEFT JOIN benefit_t ON benefit_t.TARGET_COHORT_ID = fr.TARGET_COHORT_ID AND benefit_t.OUTCOME_COHORT_ID = fr.OUTCOME_COHORT_ID
    LEFT JOIN risk_t ON risk_t.TARGET_COHORT_ID = fr.TARGET_COHORT_ID AND risk_t.OUTCOME_COHORT_ID = fr.OUTCOME_COHORT_ID
    
    INNER JOIN @schema.target t ON t.target_cohort_id = fr.target_cohort_id
    INNER JOIN @schema.outcome o ON o.outcome_cohort_id = fr.outcome_cohort_id
    --LEFT JOIN exposure_classes ec ON ec.CONCEPT_ID = tc.TARGET_CONCEPT_ID
    WHERE benefit_count IN (@benefit_selection) AND risk_count IN (@risk_selection)