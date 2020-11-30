select
    count(*) as cohort_count
from
(
  select
    co1.person_id
    , ca1.ancestor_concept_id
    , min(co1.condition_start_date) as cohort_start_date
  from @cdm_database_schema.condition_occurrence co1
  inner join @cohort_database_schema.concept_ancestor_grp ca1
    on co1.condition_concept_id = ca1.descendant_concept_id
  group by
    co1.person_id
    , ca1.ancestor_concept_id
) t1
inner join @reference_schema.@outcome_cohort_definition ocr ON (
    ocr.conceptset_id = t1.ancestor_concept_id AND ocr.outcome_type = 0
)
left join @cohort_database_schema.computed_o_cohorts coc ON ocr.cohort_definition_id = coc.cohort_definition_id
inner join
(
  select
    co1.person_id
    , ca1.ancestor_concept_id
    , min(vo1.visit_start_date) as cohort_start_date
    , max(vo1.visit_start_date) as confirmed_date
  from @cdm_database_schema.condition_occurrence co1
  inner join @cdm_database_schema.visit_occurrence vo1
    on co1.person_Id = vo1.person_id
    and co1.visit_occurrence_id = vo1.visit_occurrence_id
  inner join @cohort_database_schema.concept_ancestor_grp ca1
    on co1.condition_concept_id = ca1.descendant_concept_id
  group by
    co1.person_id
    , ca1.ancestor_concept_id
) t2
  on t1.person_id = t2.person_id
  and t1.ancestor_concept_id = t2.ancestor_concept_id
  where t2.cohort_start_date < t2.confirmed_date -- here's the piece that finds two unique visit dates
  AND coc.cohort_definition_id IS NULL -- Stop recomputing cohorts