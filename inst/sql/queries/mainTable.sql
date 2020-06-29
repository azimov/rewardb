WITH benefit_t AS(
    SELECT TARGET_COHORT_ID, OUTCOME_COHORT_ID, COUNT(DISTINCT(SOURCE_ID)) AS THRESH_COUNT
    FROM @schema.result
    WHERE RR <= @benefit
        AND P_VALUE < 0.05
        AND calibrated = @calibrated
        AND SOURCE_ID >= 0 -- NEGATIVE SOURCE IDS are reserved for meta analysis
    GROUP BY TARGET_COHORT_ID, OUTCOME_COHORT_ID
),

risk_t AS (
    SELECT TARGET_COHORT_ID, OUTCOME_COHORT_ID, COUNT(DISTINCT(SOURCE_ID)) AS THRESH_COUNT
    FROM @schema.result
    WHERE RR >= @risk
        AND P_VALUE < 0.05
        AND calibrated = @calibrated
        AND SOURCE_ID >= 0 -- NEGATIVE SOURCE IDS are reserved for meta analysis
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
    END AS benefit_count,
    mr.I2 as I2,
    ROUND(mr.RR, 2) as meta_RR,
    CASE
        WHEN nc.outcome_cohort_id IS NULL THEN 0
        ELSE 1
    END AS is_nc
FROM @schema.result fr
    
    LEFT JOIN benefit_t ON benefit_t.TARGET_COHORT_ID = fr.TARGET_COHORT_ID AND benefit_t.OUTCOME_COHORT_ID = fr.OUTCOME_COHORT_ID
    LEFT JOIN risk_t ON risk_t.TARGET_COHORT_ID = fr.TARGET_COHORT_ID AND risk_t.OUTCOME_COHORT_ID = fr.OUTCOME_COHORT_ID
    INNER JOIN @schema.target t ON t.target_cohort_id = fr.target_cohort_id
    INNER JOIN @schema.outcome o ON o.outcome_cohort_id = fr.outcome_cohort_id
    {@exclude_indications == TRUE} ? {
        LEFT JOIN @schema.positive_indication pi ON (
            pi.outcome_cohort_id = fr.outcome_cohort_id AND pi.target_cohort_id = fr.target_cohort_id
        )
    }

    LEFT JOIN @schema.negative_control nc ON (
        nc.outcome_cohort_id = fr.outcome_cohort_id AND nc.target_cohort_id = fr.target_cohort_id
    )

    --LEFT JOIN exposure_classes ec ON ec.CONCEPT_ID = tc.TARGET_CONCEPT_ID
    LEFT JOIN @schema.result mr ON (
        fr.outcome_cohort_id = mr.outcome_cohort_id AND
        fr.target_cohort_id = mr.target_cohort_id AND
        mr.calibrated = 0 AND -- BUG NO META ANALYSIS I2 fo calibrated data, but it should be the same
        mr.source_id = -99
    )
    WHERE fr.calibrated = @calibrated
    {@exclude_indications == TRUE} ? {AND pi.outcome_cohort_id IS NULL}
    AND 1 = CASE
        WHEN risk_t.THRESH_COUNT IS NULL AND 'none' IN (@risk_selection) THEN 1
        WHEN risk_t.THRESH_COUNT = 1 AND 'one' in (@risk_selection) THEN 1
        WHEN risk_t.THRESH_COUNT >= 4 AND 'all' in (@risk_selection) THEN 1
        WHEN risk_t.THRESH_COUNT > 1 AND 'most' in (@risk_selection) THEN 1
        ELSE 0
    END
    AND 1 = CASE
        WHEN benefit_t.THRESH_COUNT IS NULL AND 'none' IN (@benefit_selection) THEN 1
        WHEN benefit_t.THRESH_COUNT = 1 AND 'one' in (@benefit_selection) THEN 1
        WHEN benefit_t.THRESH_COUNT >= 4 AND 'all' in (@benefit_selection) THEN 1
        WHEN benefit_t.THRESH_COUNT > 1 AND 'most' in (@benefit_selection) THEN 1
        ELSE 0
    END
    AND o.type_id IN (@outcome_types)