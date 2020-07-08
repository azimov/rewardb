-- DK EDIT: Start of code that would take outcome cohorts (@custom_outcome_cohort_list) from user and append those to the outcome_cohort_definition_table
-- This cohort definition logic may not be consistent with the logic used for the other outcomes above (e.g., requring INP, or having 2 distinct visits)
--     it will depend on how the user defined the cohort (in ATLAS or manually)
insert into @cohort_database_schema.@outcome_cohort_definition_table
(
   cohort_definition_id
  , cohort_definition_name
  ,	short_name
  , CONCEPTSET_ID
  , outcome_type
)
select
  distinct cohort_definition_id
  , 'Custom outcome definition' as cohort_definition_name
  , 'Custom outcome definition' as short_name
  , 99999999 as CONCEPTSET_ID
  , 2 as outcome_type
from @cdm_outcome_cohort_schema.@cdm_outcome_cohort_table
where cohort_definition_id in (@custom_outcome_cohort_list)
;
------------ END of adding user defined cohorts ----------------------
