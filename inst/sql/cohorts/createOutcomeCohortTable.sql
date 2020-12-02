{@delete_existing = 1} ? {if object_id('@cohort_database_schema.@outcome_cohort_table', 'U') is not null
	drop table @cohort_database_schema.@outcome_cohort_table;
}

if object_id('@cohort_database_schema.@outcome_cohort_table', 'U') is null
    --HINT DISTRIBUTE_ON_KEY(subject_id)
    create table @cohort_database_schema.@outcome_cohort_table
    (
        cohort_definition_id bigint
        , subject_id bigint
        ,	cohort_start_date date
        ,	cohort_end_date date
    )
    ;
