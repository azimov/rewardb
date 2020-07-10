insert into @cohort_database_schema.@outcome_cohort_definition_table
(
    cohort_definition_id,
    cohort_definition_name,
    short_name,
    CONCEPTSET_ID,
    outcome_type
)
VALUES (
    @cohort_definition_id,
    @custom_outcome_name,
    @custom_outcome_name,
    99999999,
    2
);

INSERT INTO @cohort_database_schema.@outcome_cohort_table
(
  cohort_definition_id
  , subject_id
  , cohort_start_date
  , cohort_end_date
)
VALUES (
    @cohort_definition_id,
    @subject_id,
    @cohort_start_date,
    @cohort_end_date,
);
