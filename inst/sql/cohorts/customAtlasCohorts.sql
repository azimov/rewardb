INSERT INTO @cohort_database_schema.@outcome_cohort_table
(
  cohort_definition_id,
  subject_id,
  cohort_start_date,
  cohort_end_date
)
SELECT
    c.cohort_definition_id,
    c.subject_id,
    c.cohort_start_data,
    c.cohort_end_date
FROM
    @cdm_outcome_cohort_schema.cohort c
INNER JOIN @cohort_database_schema.@outcome_cohort_definition_table cdef
LEFT JOIN @cohort_database_schema.@outcome_cohort_table mt ON (
    mt.cohort_definition_id = c.cohort_definition_id AND
    mt.subject_id = c.subject_id
)
WHERE cdef.outcome_type = 2
AND  mt.subject_id IS NULL