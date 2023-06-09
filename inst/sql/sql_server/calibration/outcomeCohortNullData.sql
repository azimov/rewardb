IF OBJECT_ID('@results_schema.outcome_cohort_null_data', 'U') IS NOT NULL
    DROP TABLE @results_schema.outcome_cohort_null_data;

CREATE TABLE @results_schema.outcome_cohort_null_data AS (
    SELECT sr.*
    FROM @results_schema.outcome_negative_control_concept oncc
    INNER JOIN @results_schema.cohort_definition cd ON (cd.drug_conceptset_id = oncc.concept_id)
    INNER JOIN @results_schema.scc_result sr ON (sr.target_cohort_id = cd.cohort_definition_id AND sr.outcome_cohort_id = oncc.cohort_definition_id)
    -- Join CEM ingredients to reward cohorts
    WHERE sr.analysis_id IN (@analysis_id)
    AND sr.rr IS NOT NULL
    AND sr.t_cases + sr.c_cases >= @min_cohort_size
    {@source_ids != ''} ? {AND sr.source_id IN (@source_ids)}
)