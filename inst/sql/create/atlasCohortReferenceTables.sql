IF OBJECT_ID('@cohort_database_schema.@atlas_outcome_reference', 'U') IS NOT NULL
	DROP TABLE @cohort_database_schema.@atlas_outcome_reference;

CREATE TABLE @cohort_database_schema.@atlas_outcome_reference (
    COHORT_DEFINITION_ID BIGINT,
    COHORT_NAME VARCHAR(1000)
);

IF OBJECT_ID('@cohort_database_schema.@atlas_concept_reference', 'U') IS NOT NULL
	DROP TABLE @cohort_database_schema.@atlas_concept_reference;

CREATE TABLE @cohort_database_schema.@atlas_concept_reference (
    COHORT_DEFINITION_ID BIGINT,
    CONCEPT_ID BIGINT,
    INCLUDE_DESCENDANTS INT,
    IS_EXCLUDED INT
);