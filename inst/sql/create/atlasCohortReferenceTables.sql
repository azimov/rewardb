IF OBJECT_ID('@cohort_database_schema.@atlas_reference_table', 'U') IS NOT NULL
	DROP TABLE @cohort_database_schema.@atlas_reference_table;

CREATE TABLE @cohort_database_schema.@atlas_reference_table (
    COHORT_DEFINITION_ID BIGINT NOT NULL PRIMARY KEY,
    COHORT_NAME VARCHAR,
) with (distribution = replicate);

IF OBJECT_ID('@cohort_database_schema.@atlas_concept_reference', 'U') IS NOT NULL
	DROP TABLE @cohort_database_schema.@atlas_concept_reference;

CREATE TABLE @cohort_database_schema.@atlas_concept_reference (
    COHORT_DEFINITION_ID BIGINIT,
    CONCEPT_ID BIGINT,
    INCLUDE_DESCENDANTS BIT,
    IS_EXCLUDED BIT,
    PRIMARY KEY(COHORT_DEFINITION_ID, CONCEPT_ID)
) with (distribution = replicate);