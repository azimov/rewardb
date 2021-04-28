-- Get subset of outcomes for a given exposure that have no evidence associated with them
SELECT ms.ingredient_concept_id as exposure_id,
  sr.*
  FROM @cem.matrix_summary ms
  INNER JOIN @results_schema.cohort_definition cd ON cd.drug_conceptset_id = ms.ingredient_concept_id
  INNER JOIN @results_schema.scc_result sr ON sr.target_cohort_id = cd.cohort_definition_id
  INNER JOIN @results_schema.outcome_cohort_definition ocd ON (ocd.cohort_definition_id = sr.outcome_cohort_id AND ocd.conceptset_id = ms.condition_concept_id)
  WHERE cd.cohort_definition_id IN (@exposure_ids)
  AND cd.atc_flg = 0
  AND sr.analysis_id = @analysis_id
  AND ocd.outcome_type = 0
  AND ms.evidence_exists = 0
  AND sr.rr IS NOT NULL
  AND sr.t_cases > 0
  AND sr.c_cases > 0

  UNION

SELECT mssq.exposure_id,
  sr.*
  FROM (
    SELECT
      cd.cohort_definition_id,
      cd.drug_conceptset_id as exposure_id,
      ms.condition_concept_id,
      max(evidence_exists) as evidence_exists
    FROM @cem.matrix_summary ms
    INNER JOIN @vocabulary_schema.concept_ancestor ca ON ca.descendant_concept_id = ms.ingredient_concept_id
    INNER JOIN @results_schema.cohort_definition cd ON ca.ancestor_concept_id = cd.drug_conceptset_id
    WHERE cd.cohort_definition_id IN (@exposure_ids)
    AND cd.atc_flg != 0
    GROUP BY cd.cohort_definition_id, cd.drug_conceptset_id, ms.condition_concept_id
  ) mssq
  INNER JOIN @results_schema.scc_result sr ON sr.target_cohort_id = mssq.cohort_definition_id
  INNER JOIN @results_schema.outcome_cohort_definition ocd ON (
    ocd.cohort_definition_id = sr.outcome_cohort_id AND ocd.conceptset_id = mssq.condition_concept_id
  )
  WHERE mssq.evidence_exists = 0
  AND sr.analysis_id = @analysis_id
  AND ocd.outcome_type = 0
  AND sr.rr IS NOT NULL
  AND sr.t_cases > 0
  AND sr.c_cases > 0