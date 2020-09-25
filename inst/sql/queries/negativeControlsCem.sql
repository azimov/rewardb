SELECT
     o.outcome_cohort_id,
     t.target_cohort_id,
     max(evi.EVIDENCE_EXISTS) as evidence
FROM @schema.@summary_table evi
INNER JOIN @schema.CONCEPT_ANCESTOR ca1 ON ( ca1.ancestor_concept_id = evi.condition_concept_id )
INNER JOIN @outcome_cohort_table o ON ( ca1.descendant_concept_id = o.outcome_concept_id )
INNER JOIN @schema.concept_ancestor ca2 ON( ca2.ancestor_concept_id = evi.ingredient_concept_id )
INNER JOIN @target_cohort_table t ON ( ca2.descendant_concept_id = t.target_concept_id )

GROUP BY o.outcome_cohort_id, t.target_cohort_id