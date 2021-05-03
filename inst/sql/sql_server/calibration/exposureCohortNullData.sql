
SELECT sr.*
    FROM (
        -- All child concepts merged
        SELECT
            ocd.cohort_definition_id,
            ms.ingredient_concept_id,
            max(ms.evidence_exists) as evidence_exists
        FROM @cem.matrix_summary ms
        INNER JOIN @vocabulary.concept_ancestor ca ON ca.descendant_concept_id = ms.condition_concept_id
        INNER JOIN @results_schema.outcome_cohort_definition ocd ON ocd.conceptset_id = ca.ancestor_concept_id
        WHERE ocd.cohort_definition_id IN (@outcome_ids)
        AND ocd.outcome_type != 2 -- Not atlas definitions
        GROUP BY ocd.cohort_definition_id, ms.ingredient_concept_id

        UNION
        -- Atlas cohorts, merge all child concepts into a single cohort
        SELECT
            ocd.cohort_definition_id,
            ms.ingredient_concept_id,
            max(ms.evidence_exists) as evidence_exists
        FROM @cem.matrix_summary ms
        INNER JOIN @vocabulary.concept_ancestor ca ON ca.descendant_concept_id = ms.condition_concept_id
        INNER JOIN @results_schema.atlas_outcome_concept aoc ON aoc.concept_id = ca.ancestor_concept_id
        INNER JOIN @results_schema.outcome_cohort_definition ocd ON ocd.cohort_definition_id = aoc.cohort_definition_id
        WHERE ocd.cohort_definition_id IN (@outcome_ids)
        GROUP BY ocd.cohort_definition_id, ms.ingredient_concept_id

    ) mssq
    INNER JOIN @results_schema.scc_result sr ON sr.outcome_cohort_id = mssq.cohort_definition_id
    -- Join CEM ingredients to reward cohorts
    INNER JOIN @results_schema.cohort_definition cd ON (cd.drug_conceptset_id = mssq.ingredient_concept_id AND cd.cohort_definition_id = sr.target_cohort_id)
    WHERE sr.analysis_id = @analysis_id
    AND mssq.evidence_exists = 0
    AND sr.rr IS NOT NULL
    AND sr.t_cases > 0
    AND sr.c_cases > 0
    AND sr.t_cases + sr.c_cases >= @min_cohort_size