/* CONCEPT SET TABLE */
IF OBJECT_ID('@cohort_database_schema.@conceptset_definition_table', 'U') IS NOT NULL
  TRUNCATE TABLE @cohort_database_schema.@conceptset_definition_table;
	DROP TABLE @cohort_database_schema.@conceptset_definition_table;

create table @cohort_database_schema.@conceptset_definition_table
(
	CONCEPTSET_ID bigint,
	conceptset_name varchar(1000),
	concept_id bigint,
	isExcluded bit,
	includeDescendants bit,
	includeMapped bit
) with (distribution=replicate)
;

/* COHORT DEFINITION TABLE */
IF OBJECT_ID('@cohort_database_schema.@cohort_definition_table', 'U') IS NOT NULL
  TRUNCATE TABLE @cohort_database_schema.@cohort_definition_table;
	DROP TABLE @cohort_database_schema.@cohort_definition_table;

create table @cohort_database_schema.@cohort_definition_table
(
	cohort_definition_id bigint
	, cohort_definition_name varchar(1000)
	,	short_name varchar(1000)
	,	drug_CONCEPTSET_ID bigint
	,	indication_CONCEPTSET_ID bigint
	,	target_cohort int   	/*1 - target cohort, 0- not target (could be used as comparator, -1 - other)*/
	, subgroup_cohort int   /*tells if cohort is a subgroup of interest: 0-none, 1-pediatrics, 2-elderly, 3-pregnant women, 4-renal impairment,5-hepatic impairment*/
	, ATC_flg int           /* 1- exposure is ATC 4th level, 0-exposure is ingredient */
) with (distribution=replicate)
;

/* OUTCOME COHORT DEFINITION TABLE */
IF OBJECT_ID('@cohort_database_schema.@outcome_cohort_definition_table', 'U') IS NOT NULL
  TRUNCATE TABLE @cohort_database_schema.@outcome_cohort_definition_table;
	DROP TABLE @cohort_database_schema.@outcome_cohort_definition_table;

create table @cohort_database_schema.@outcome_cohort_definition_table
(
	cohort_definition_id bigint
	, cohort_definition_name varchar(1000)
	,	short_name varchar(1000)
	, CONCEPTSET_ID bigint
	, outcome_type int /*0-outcomes for 2+ diagnoses, 1-hospitalizations, 2-custom outcome cohort*/
) with (distribution=replicate)
;
