WITH benefit_t AS(
    SELECT TARGET_COHORT_ID, OUTCOME_COHORT_ID, COUNT(DISTINCT(SOURCE_ID)) AS sc_benefit_count
    FROM full_results
    WHERE RR <= @benefitThreshold
        AND P_VALUE < 0.05
    GROUP BY TARGET_COHORT_ID, OUTCOME_COHORT_ID
), harm_t AS (
    SELECT TARGET_COHORT_ID, OUTCOME_COHORT_ID, COUNT(DISTINCT(SOURCE_ID)) AS sc_harm_count
    FROM full_results
    WHERE RR >= @harmThreshold
        AND P_VALUE < 0.05
    GROUP BY TARGET_COHORT_ID, OUTCOME_COHORT_ID
)

SELECT fr.TARGET_COHORT_ID, fr.TARGET_COHORT_NAME, fr.OUTCOME_COHORT_ID, fr.OUTCOME_COHORT_NAME, tc.EXPOSURE_CLASS,
    CASE
        WHEN harm_t.sc_harm_count IS NULL THEN 'none'
        WHEN harm_t.sc_harm_count = 1 THEN 'one'
        WHEN harm_t.sc_harm_count = 5 THEN 'all'
        WHEN harm_t.sc_harm_count > 1 THEN 'most'
        ELSE harm_t.sc_harm_count
    END AS sc_risk,
    CASE
        WHEN benefit_t.sc_benefit_count IS NULL THEN 'none'
        WHEN benefit_t.sc_benefit_count = 1 THEN 'one'
        WHEN benefit_t.sc_benefit_count = 5 THEN 'all'
        WHEN benefit_t.sc_benefit_count > 1 THEN 'most'
        ELSE benefit_t.sc_benefit_count
    END AS sc_benefit
FROM full_results fr
    LEFT JOIN harm_t ON harm_t.TARGET_COHORT_ID = fr.TARGET_COHORT_ID AND harm_t.OUTCOME_COHORT_ID = fr.OUTCOME_COHORT_ID
    LEFT JOIN benefit_t ON benefit_t.TARGET_COHORT_ID = fr.TARGET_COHORT_ID AND benefit_t.OUTCOME_COHORT_ID = fr.OUTCOME_COHORT_ID
    INNER JOIN TREATMENT_CLASSES tc ON fr.TARGET_COHORT_ID = tc.TARGET_COHORT_ID
GROUP BY fr.TARGET_COHORT_ID, fr.OUTCOME_COHORT_ID