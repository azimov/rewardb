WITH outcome_cohort_concept AS (
    SELECT cohort_definition_id as outcome_cohort_id, conceptset_id as outcome_concept_id
    FROM @schema.outcome_cohort_definition
    WHERE OUTCOME_TYPE IN (0,1)
    {@outcome_cohort_ids != ''} ? {AND cohort_definition_id IN (@outcome_cohort_ids)}

    UNION

    SELECT cohort_definition_id as outcome_cohort_id, conceptset_id as outcome_concept_id
    FROM @schema.outcome_cohort_definition
    WHERE OUTCOME_TYPE IN (0,1)
    {@outcome_cohort_ids != ''} ? {AND cohort_definition_id IN (@outcome_cohort_ids)}

    UNION

    SELECT COHORT_DEFINITION_ID as outcome_cohort_id, CONCEPT_ID as outcome_concept_id
    FROM @schema.atlas_concept_reference
    {@outcome_cohort_ids != ''} ? {WHERE cohort_definition_id IN (@outcome_cohort_ids)}
),

target_cohort_concept AS (
    SELECT
        cohort_definition_id as target_cohort_id,
        drug_conceptset_id as target_concept_id,
        ATC_flg as is_atc_4
    FROM @schema.cohort_definition
    WHERE ATC_flg IN (0, 1)
    {@target_cohort_ids != ''} ? {AND cohort_definition_id IN (@target_cohort_ids)}

    UNION

    SELECT cohort_definition_id as target_cohort_id, concept_id as target_concept_id, 0 as is_atc_4
    FROM @schema.custom_exposure_concept
    {@target_cohort_ids != ''} ? {WHERE cohort_definition_id IN (@target_cohort_ids)}
)


-- Top level conditions that match out hierarchy, we assume that siblings do not have evidence
SELECT o.outcome_cohort_id, t.target_cohort_id, max(evi.evidence_exists) as evidence
    FROM @cem_schema.@summary_table evi
    INNER JOIN target_cohort_concept t ON t.target_concept_id = evi.ingredient_concept_id
    INNER JOIN outcome_cohort_concept o ON o.outcome_concept_id = evi.condition_concept_id
    WHERE t.is_atc_4 = 0
    GROUP BY o.outcome_cohort_id, t.target_cohort_id
UNION
(
-- Roll up any descendent conditions
SELECT o.outcome_cohort_id, t.target_cohort_id, max(evi.evidence_exists) as evidence
    FROM @cem_schema.@summary_table evi
    INNER JOIN target_cohort_concept t ON t.target_concept_id = evi.ingredient_concept_id
    INNER JOIN @vocab_schema.concept_ancestor ca ON ca.ancestor_concept_id = evi.condition_concept_id
    INNER JOIN outcome_cohort_concept o ON o.outcome_concept_id = ca.descendant_concept_id
    WHERE t.is_atc_4 = 0
    GROUP BY o.outcome_cohort_id, t.target_cohort_id
)
UNION
-- GET all ATC level 4 concept mappings
(
SELECT o.outcome_cohort_id, t.target_cohort_id, max(evi.evidence_exists) as evidence
    FROM @cem_schema.@summary_table evi
    INNER JOIN outcome_cohort_concept o ON o.outcome_concept_id = evi.condition_concept_id
    INNER JOIN @vocab_schema.concept_ancestor ca ON ca.descendant_concept_id = evi.ingredient_concept_id
    INNER JOIN target_cohort_concept t ON t.target_concept_id =  ca.ancestor_concept_id
    INNER JOIN @vocab_schema.concept c ON (c.concept_id = evi.ingredient_concept_id AND c.concept_class_id = 'Ingredient')
    WHERE t.is_atc_4 = 1
    GROUP BY o.outcome_cohort_id, t.target_cohort_id
)