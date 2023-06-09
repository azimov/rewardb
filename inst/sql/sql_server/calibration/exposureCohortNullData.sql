IF OBJECT_ID('@results_schema.exposure_cohort_null_data', 'U') IS NOT NULL
    DROP TABLE @results_schema.exposure_cohort_null_data;

CREATE TABLE @results_schema.exposure_cohort_null_data AS (
    SELECT sr.*, ocd.outcome_type
    FROM @results_schema.exposure_negative_control_concept encc
    INNER JOIN @results_schema.cohort_definition cd on encc.cohort_definition_id = cd.cohort_definition_id
    INNER JOIN @results_schema.outcome_cohort_definition ocd ON (ocd.conceptset_id = encc.concept_id)
    INNER JOIN @results_schema.scc_result sr ON (sr.outcome_cohort_id = ocd.cohort_definition_id AND sr.target_cohort_id = encc.cohort_definition_id)
    -- Join CEM ingredients to reward cohorts
    WHERE sr.analysis_id IN (@analysis_id)
    AND sr.rr IS NOT NULL
    AND sr.t_cases + sr.c_cases >= @min_cohort_size
    {@source_ids != ''} ? {AND sr.source_id IN (@source_ids)}
)