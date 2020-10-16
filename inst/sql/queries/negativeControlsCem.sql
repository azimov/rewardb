-- Top level conditions that match out hierarchy, we assume that siblings do not have evidence
SELECT o.outcome_cohort_id, t.target_cohort_id, max(evi.evidence_exists) as evidence
    FROM @schema.@summary_table evi
    INNER JOIN @target_cohort_table t ON t.target_concept_id = evi.ingredient_concept_id
    INNER JOIN @outcome_cohort_table o ON o.outcome_concept_id = evi.condition_concept_id
    WHERE t.is_atc_4 = 0
    GROUP BY o.outcome_cohort_id, t.target_cohort_id
UNION
(
-- Roll up any descendent conditions
SELECT o.outcome_cohort_id, t.target_cohort_id, max(evi.evidence_exists) as evidence
    FROM @schema.@summary_table evi
    INNER JOIN @target_cohort_table t ON t.target_concept_id = evi.ingredient_concept_id
    INNER JOIN @vocab_schema.concept_ancestor ca ON ca.ancestor_concept_id = evi.condition_concept_id
    INNER JOIN @outcome_cohort_table o ON o.outcome_concept_id = ca.descendant_concept_id
    WHERE t.is_atc_4 = 0
    GROUP BY o.outcome_cohort_id, t.target_cohort_id
)
UNION
-- GET all ATC level 4 concept mappings
(
SELECT o.outcome_cohort_id, t.target_cohort_id, max(evi.evidence_exists) as evidence
    FROM @schema.@summary_table evi
    INNER JOIN @outcome_cohort_table o ON o.outcome_concept_id = evi.condition_concept_id
    INNER JOIN @vocab_schema.concept_ancestor ca ON ca.descendant_concept_id = evi.ingredient_concept_id
    INNER JOIN @target_cohort_table t ON t.target_concept_id =  ca.ancestor_concept_id
    INNER JOIN @vocab_schema.concept c ON (c.concept_id = evi.ingredient_concept_id AND c.concept_class_id = 'Ingredient')
    WHERE t.is_atc_4 = 1
    GROUP BY o.outcome_cohort_id, t.target_cohort_id
)