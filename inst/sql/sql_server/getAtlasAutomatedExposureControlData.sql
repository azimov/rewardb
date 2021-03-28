with ingredient_evidence as (
   select ms.ingredient_concept_id, aor.atlas_id, aor.cohort_definition_id, max(evidence_exists) as evidence_exists
   from @cem_schema.matrix_summary ms
   inner join @vocabulary.concept_ancestor ca on ca.descendant_concept_id = ms.condition_concept_id
   inner join @schema.atlas_outcome_concept aoc on aoc.concept_id = ca.ancestor_concept_id
   inner join @schema.atlas_outcome_reference aor on aoc.cohort_definition_id = aor.cohort_definition_id
   where aor.atlas_url = @source_url
   and aor.atlas_id in (@atlas_ids)
   group by ms.ingredient_concept_id, aor.atlas_id, aor.cohort_definition_id
)

select sr.*, ie.atlas_id
from ingredient_evidence ie
inner join @schema.cohort_definition cd on cd.drug_conceptset_id = ie.ingredient_concept_id
inner join @schema.scc_result sr on sr.outcome_cohort_id = ie.cohort_definition_id and sr.target_cohort_id = cd.cohort_definition_id
where ie.evidence_exists = 0