-- Top level conditions that match out hierarchy, we assume that siblings do not have evidence
SELECT DISTINCT evi.INGREDIENT_CONCEPT_ID AS INGREDIENT_CONCEPT_ID, evi.CONDITION_CONCEPT_ID AS condition_concept_id, max(evi.evidence_exists) as evidence
    FROM @schema.@summary_table evi
    INNER JOIN #target_nc_tmp ttmp ON ttmp.target_concept_id = evi.ingredient_concept_id
    INNER JOIN #outcome_nc_tmp otmp ON otmp.condition_concept_id = evi.condition_concept_id
    WHERE ttmp.is_atc_4 = 0
    GROUP BY evi.INGREDIENT_CONCEPT_ID, evi.CONDITION_CONCEPT_ID
UNION
(
-- Roll up any descendent conditions
SELECT DISTINCT evi.INGREDIENT_CONCEPT_ID AS INGREDIENT_CONCEPT_ID, otmp.condition_concept_id AS condition_concept_id, max(evi.evidence_exists) as evidence
    FROM @schema.@summary_table evi
    INNER JOIN #target_nc_tmp ttmp ON ttmp.target_concept_id = evi.ingredient_concept_id
    INNER JOIN @vocab_schema.concept_ancestor ca ON ca.ancestor_concept_id = evi.condition_concept_id
    INNER JOIN #outcome_nc_tmp otmp ON otmp.condition_concept_id = ca.descendant_concept_id
    WHERE ttmp.is_atc_4 = 0
    GROUP BY evi.INGREDIENT_CONCEPT_ID, otmp.condition_concept_id
)
UNION
-- GET all ATC level 4 concept mappings
(
SELECT DISTINCT ttmp.target_concept_id AS INGREDIENT_CONCEPT_ID, evi.CONDITION_CONCEPT_ID AS condition_concept_id, max(evi.evidence_exists) as evidence
    FROM @schema.@summary_table evi
    INNER JOIN #outcome_nc_tmp otmp ON otmp.condition_concept_id = evi.condition_concept_id
    INNER JOIN @vocab_schema.concept_ancestor ca ON ca.descendant_concept_id = evi.ingredient_concept_id
    INNER JOIN #target_nc_tmp ttmp ON ttmp.target_concept_id = ca.ancestor_concept_id
    INNER JOIN @vocab_schema.concept c ON (c.concept_id = evi.ingredient_concept_id AND c.concept_class_id = 'Ingredient')
    WHERE ttmp.is_atc_4 = 1
    GROUP BY ttmp.target_concept_id, evi.CONDITION_CONCEPT_ID
)