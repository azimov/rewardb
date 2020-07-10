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
    'Custom outcome definition',
    'Custom outcome definition',
    99999999,
    2
)"