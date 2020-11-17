{DEFAULT @schema = 'reward'}
{DEFAULT @concept_set_definition = 'concept_set_definition'}
{DEFAULT @cohort_definition = 'cohort_definition'}
{DEFAULT @outcome_cohort_definition = 'outcome_cohort_definition'}
{DEFAULT @atlas_outcome_reference = 'atlas_outcome_reference'}
{DEFAULT @atlas_concept_reference = 'atlas_concept_reference'}
{DEFAULT @custom_exposure = 'custom_exposure'}
{DEFAULT @custom_exposure_concept = 'custom_exposure_concept'}

/* CONCEPT SET TABLE */
create table @schema.@concept_set_definition
(
	CONCEPTSET_ID bigint,
	concept_name varchar(1000),
	concept_id bigint,
	is_excluded INT,
	include_Descendants INT,
	include_Mapped INT
)
;

/* COHORT DEFINITION TABLE */
create table @schema.@cohort_definition
(
	cohort_definition_id SERIAL PRIMARY KEY
	, cohort_definition_name varchar(1000)
	,	short_name varchar(1000)
	,	drug_CONCEPTSET_ID bigint
	,	target_cohort int   	/*1 - target cohort, 0- not target (could be used as comparator, -1 - other)*/
	, subgroup_cohort int   /*tells if cohort is a subgroup of interest: 0-none, 1-pediatrics, 2-elderly, 3-pregnant women, 4-renal impairment,5-hepatic impairment*/
	, ATC_flg int           /* 1- exposure is ATC 4th level, 0-exposure is ingredient 2 - custom exposure class */
)
;

/* OUTCOME COHORT DEFINITION TABLE */
create table @schema.@outcome_cohort_definition
(
	cohort_definition_id SERIAL PRIMARY KEY
	, cohort_definition_name varchar(1000)
	,	short_name varchar(1000)
	, CONCEPTSET_ID bigint
	, outcome_type int /*0-outcomes for 2+ diagnoses, 1-hospitalizations, 2-custom outcome cohort*/
)
;


CREATE TABLE @schema.@atlas_outcome_reference (
    COHORT_DEFINITION_ID BIGINT,
    ATLAS_ID BIGINT,
    sql_definition text,
    DEFINITION text, -- Stores sql used to generate the cohort
    atlas_url varchar(1000), -- Base atlas url used to pull cohort
    CONSTRAINT cohort_def
      FOREIGN KEY(COHORT_DEFINITION_ID)
	    REFERENCES @schema.@outcome_cohort_definition(COHORT_DEFINITION_ID)
	        ON DELETE CASCADE

);

CREATE TABLE @schema.@atlas_concept_reference (
    COHORT_DEFINITION_ID INT,
    CONCEPT_ID BIGINT,
    INCLUDE_DESCENDANTS INT,
    IS_EXCLUDED INT,
    INCLUDE_MAPPED INT,
    CONSTRAINT cohort_def
      FOREIGN KEY(COHORT_DEFINITION_ID)
	    REFERENCES @schema.@outcome_cohort_definition(COHORT_DEFINITION_ID)
	        ON DELETE CASCADE
);


CREATE TABLE @schema.@custom_exposure (
    COHORT_DEFINITION_ID BIGINT,
    CONCEPT_SET_ID INT,
    ATLAS_URL varchar(1000),
    CONSTRAINT cohort_def
      FOREIGN KEY(COHORT_DEFINITION_ID)
	    REFERENCES @schema.@cohort_definition(COHORT_DEFINITION_ID)
	        ON DELETE CASCADE
);

CREATE TABLE @schema.@custom_exposure_concept (
    COHORT_DEFINITION_ID BIGINT,
    CONCEPT_ID BIGINT,
    INCLUDE_DESCENDANTS INT,
    IS_EXCLUDED INT,
    INCLUDE_MAPPED INT,
    CONSTRAINT cohort_def
      FOREIGN KEY(COHORT_DEFINITION_ID)
	    REFERENCES @schema.@cohort_definition(COHORT_DEFINITION_ID)
	        ON DELETE CASCADE
);
