    SELECT DISTINCT ca2.descendant_concept_id AS condition_concept_id, evi.INGREDIENT_CONCEPT_ID, max(evi.EVIDENCE_EXISTS) as evidence
        FROM @schema.@summary_table evi
        INNER JOIN @schema.CONCEPT_ANCESTOR ca ON ( ca.descendant_concept_id = evi.CONDITION_CONCEPT_ID)
        INNER JOIN @schema.CONCEPT_ANCESTOR ca2 ON (
         ca.ancestor_concept_id = ca2.ancestor_concept_id AND ca2.max_levels_of_separation = 1
        )
        INNER JOIN #outcome_nc_tmp otmp ON otmp.condition_concept_id = ca2.descendant_concept_id
        INNER JOIN #target_nc_tmp ttmp ON ttmp.target_concept_id = evi.ingredient_concept_id

        WHERE ttmp.is_atc_4 = 0
        GROUP BY ca2.descendant_concept_id, evi.INGREDIENT_CONCEPT_ID

UNION

    SELECT DISTINCT ca2.descendant_concept_id AS condition_concept_id, ttmp.target_concept_id AS ingredient_concept_id, max(evi.EVIDENCE_EXISTS) as evidence
        FROM @schema.@summary_table evi
        INNER JOIN @schema.CONCEPT_ANCESTOR ca ON ( ca.descendant_concept_id = evi.CONDITION_CONCEPT_ID)
        INNER JOIN @schema.CONCEPT_ANCESTOR ca2 ON (
         ca.ancestor_concept_id = ca2.ancestor_concept_id AND ca2.max_levels_of_separation = 1
        )
        INNER JOIN #outcome_nc_tmp otmp ON otmp.condition_concept_id = ca2.descendant_concept_id

        INNER JOIN @schema.concept_ancestor ca3 ON ca3.descendant_concept_id = evi.ingredient_concept_id
        INNER JOIN #target_nc_tmp ttmp ON ttmp.target_concept_id = ca3.ancestor_concept_id

        WHERE ttmp.is_atc_4 = 1
        GROUP BY ca2.descendant_concept_id, ttmp.target_concept_id