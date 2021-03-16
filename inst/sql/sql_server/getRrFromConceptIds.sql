WITH outcome_cohort AS (
    SELECT cohort_definition_id, conceptset_id AS concept_id
    FROM @reference_schema.outcome_cohort_definition ocd
    INNER JOIN vocabulary.concept_ancestor ca ON ca.descendant_concept_id = ocd.conceptset_id
    WHERE ca.ancestor_concept_id IN (@outcome_concept_ids)

    UNION

    SELECT aoc.cohort_definition_id, aoc.concept_id FROM @reference_schema.atlas_outcome_concept aoc
    WHERE aoc.concept_id IN (@outcome_concept_ids)
    AND aoc.is_excluded = 0

    UNION

    SELECT cohort_definition_id, ca.descendant_concept_id as concept_id FROM @reference_schema.atlas_outcome_concept aoc
    INNER JOIN vocabulary.concept_ancestor ca ON ca.descendant_concept_id = aoc.concept_id
    WHERE ca.ancestor_concept_id IN (@outcome_concept_ids)
    AND aoc.is_excluded = 0

    AND aoc.include_descendants = 1

),
target_cohort AS (
    SELECT cohort_definition_id, csd.concept_id FROM @reference_schema.cohort_definition cr
    INNER JOIN @reference_schema.concept_set_definition csd ON cr.drug_conceptset_id = csd.conceptset_id
    WHERE csd.concept_id IN (@exposure_concept_ids)

    UNION

    -- Any parent exposure concepts
    SELECT cohort_definition_id, ca.descendant_concept_id as concept_id FROM @reference_schema.atlas_exposure_concept aec
    INNER JOIN vocabulary.concept_ancestor ca ON ca.ancestor_concept_id = aec.concept_id
    WHERE ca.descendant_concept_id IN (@exposure_concept_ids)
    AND aec.is_excluded = 0
    AND aec.include_descendants = 1
),
exposure_outcome_cohorts AS (
    SELECT
        eop.outcome_concept_id,
        eop.exposure_concept_id,
        tc.cohort_definition_id AS target_cohort_id,
        oc.cohort_definition_id AS outcome_cohort_id
    FROM o_exp_pair eop
    INNER JOIN target_cohort tc ON tc.concept_id = eop.exposure_concept_id
    INNER JOIN outcome_cohort oc ON oc.concept_id = eop.outcome_concept_id
)

SELECT
    eoc.exposure_concept_id,
    eoc.outcome_concept_id,
    ocd.short_name as outcome_cohort_name,
    cd.short_name as exposure_cohort_name,
    sr.*
FROM @results_schema.scc_result sr
INNER JOIN exposure_outcome_cohorts eoc ON (
    eoc.outcome_cohort_id = sr.outcome_cohort_id
    AND eoc.target_cohort_id = sr.target_cohort_id
)
INNER JOIN @results_schema.outcome_cohort_definition ocd ON ocd.cohort_definition_id = sr.outcome_cohort_id
INNER JOIN @results_schema.cohort_definition cd ON cd.cohort_definition_id = sr.target_cohort_id
WHERE ocd.outcome_type IN (@outcome_types)