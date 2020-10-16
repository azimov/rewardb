SELECT DISTINCT o.outcome_cohort_id, t.target_cohort_id, max(evi.EVIDENCE_EXISTS) as evidence
    FROM @schema.@summary_table evi
    INNER JOIN @vocab_schema.CONCEPT_ANCESTOR ca ON ( ca.descendant_concept_id = evi.CONDITION_CONCEPT_ID)
    INNER JOIN @vocab_schema.CONCEPT_ANCESTOR ca2 ON (
     ca.ancestor_concept_id = ca2.ancestor_concept_id AND ca2.max_levels_of_separation = 1
    )
    INNER JOIN @outcome_cohort_table o ON o.outcome_concept_id = ca2.descendant_concept_id
    INNER JOIN @target_cohort_table t ON t.target_concept_id = evi.ingredient_concept_id

    WHERE t.is_atc_4 = 0
    GROUP BY o.outcome_cohort_id, t.target_cohort_id

UNION
    -- ATC 4 level ingredients
SELECT o.outcome_cohort_id, t.target_cohort_id, max(evi.EVIDENCE_EXISTS) as evidence
    FROM @schema.@summary_table evi
    INNER JOIN @vocab_schema.CONCEPT_ANCESTOR ca ON ( ca.descendant_concept_id = evi.CONDITION_CONCEPT_ID)
    INNER JOIN @vocab_schema.CONCEPT_ANCESTOR ca2 ON (
     ca.ancestor_concept_id = ca2.ancestor_concept_id AND ca2.max_levels_of_separation = 1
    )
    INNER JOIN @outcome_cohort_table o ON o.outcome_concept_id = ca2.descendant_concept_id

    INNER JOIN @vocab_schema.concept_ancestor ca3 ON ca3.descendant_concept_id = evi.ingredient_concept_id
    INNER JOIN @target_cohort_table t ON t.target_concept_id = ca3.ancestor_concept_id

    WHERE t.is_atc_4 = 1
    GROUP BY o.outcome_cohort_id, t.target_cohort_id