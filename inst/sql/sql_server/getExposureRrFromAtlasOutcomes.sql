WITH target_cohort AS (
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
        oc.atlas_id,
        eop.exposure_concept_id,
        tc.cohort_definition_id AS target_cohort_id,
        oc.cohort_definition_id AS outcome_cohort_id
    FROM o_exp_pair eop
    INNER JOIN target_cohort tc ON tc.concept_id = eop.exposure_concept_id
    INNER JOIN @results_schema.atlas_outcome_reference oc ON oc.atlas_id = eop.atlas_id AND oc.atlas_url = eop.atlas_url
)


SELECT
    eoc.exposure_concept_id,
    eoc.atlas_id,
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
