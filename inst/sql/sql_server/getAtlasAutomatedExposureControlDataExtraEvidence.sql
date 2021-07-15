select sr.*, ie.atlas_id
from scratch.extra_evidence ie
inner join @schema.cohort_definition cd on cd.drug_conceptset_id = ie.ingredient_concept_id
inner join @schema.scc_result sr on (sr.outcome_cohort_id = ie.cohort_definition_id and sr.target_cohort_id = cd.cohort_definition_id)
where ie.evidence_exists = 0
