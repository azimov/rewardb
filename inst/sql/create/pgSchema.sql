{DEFAULT @schema = 'reward'}

DROP SCHEMA IF EXISTS @schema CASCADE;
CREATE SCHEMA @schema;

CREATE TABLE @schema.scc_result (
  source_id INT NOT NULL,
  outcome_cohort_id BIGINT NOT NULL,
  target_cohort_id BIGINT NOT NULL,
  rr NUMERIC,
  se_log_rr NUMERIC,
  c_pt NUMERIC,
  t_pt NUMERIC,
  t_at_risk NUMERIC,
  c_at_risk NUMERIC,
  t_cases NUMERIC,
  c_cases NUMERIC,
  lb_95 NUMERIC,
  ub_95 NUMERIC,
  p_value NUMERIC,
  I2 NUMERIC
);


CREATE TABLE @schema.data_source (
    source_id INT PRIMARY KEY,
    source_name varchar,
    source_key varchar
);

-- Common evidence model for control mappings
DROP SCHEMA IF EXISTS cem CASCADE;
CREATE SCHEMA cem;

CREATE TABLE cem.matrix_summary (
    ingredient_concept_id INT NOT NULL,
    condition_concept_id INT NOT NULL,
    EVIDENCE_EXISTS INT,
    PRIMARY KEY(ingredient_concept_id, condition_concept_id)
);


/* CONCEPT SET TABLE */
create table @schema.concept_set_definition
(
	CONCEPTSET_ID bigint,
	conceptset_name varchar(1000),
	concept_id bigint,
	isExcluded INT,
	includeDescendants INT,
	includeMapped INT
)
;

/* COHORT DEFINITION TABLE */
create table @schema.cohort_definition
(
	cohort_definition_id SERIAL PRIMARY KEY
	, cohort_definition_name varchar(1000)
	,	short_name varchar(1000)
	,	drug_CONCEPTSET_ID bigint
	,	indication_CONCEPTSET_ID bigint
	,	target_cohort int   	/*1 - target cohort, 0- not target (could be used as comparator, -1 - other)*/
	, subgroup_cohort int   /*tells if cohort is a subgroup of interest: 0-none, 1-pediatrics, 2-elderly, 3-pregnant women, 4-renal impairment,5-hepatic impairment*/
	, ATC_flg int           /* 1- exposure is ATC 4th level, 0-exposure is ingredient */
)
;

/* OUTCOME COHORT DEFINITION TABLE */
create table @schema.outcome_cohort_definition
(
	cohort_definition_id SERIAL PRIMARY KEY
	, cohort_definition_name varchar(1000)
	,	short_name varchar(1000)
	, CONCEPTSET_ID bigint
	, outcome_type int /*0-outcomes for 2+ diagnoses, 1-hospitalizations, 2-custom outcome cohort*/
)
;


CREATE TABLE @schema.atlas_reference_table (
    COHORT_DEFINITION_ID BIGINT,
    ATLAS_COHORT_ID INT,
    COHORT_NAME VARCHAR(1000)
);

CREATE TABLE @schema.atlas_concept_reference (
    COHORT_DEFINITION_ID BIGINT,
    CONCEPT_ID BIGINT,
    INCLUDE_DESCENDANTS INT,
    IS_EXCLUDED INT
);


-- Create ingredient cohorts from vocabulary

DELETE FROM @schema.concept_set_definition;
INSERT INTO @schema.concept_set_definition (
    CONCEPT_ID,
    CONCEPTSET_NAME,
    CONCEPTSET_ID,
    ISEXCLUDED,
    INCLUDEDESCENDANTS,
    INCLUDEMAPPED
)
SELECT DISTINCT concept_id , concept_name, concept_id, 0, 1, 0
    from @vocabulary_schema.concept
    where (concept_class_id = 'Ingredient' AND vocabulary_id = 'RxNorm' AND standard_concept = 'S')
       OR (concept_class_id = 'ATC 4th' AND vocabulary_id = 'ATC');

INSERT INTO @schema.cohort_definition (
    COHORT_DEFINITION_NAME,
    SHORT_NAME,
    DRUG_CONCEPTSET_ID,
    INDICATION_CONCEPTSET_ID,
    TARGET_COHORT,
    SUBGROUP_COHORT,
    ATC_FLG
)
SELECT DISTINCT
    CONCEPT_NAME,
    CONCAT(VOCABULARY_ID, ' - ', CONCEPT_NAME),
    CONCEPT_ID,
    0,
    0,
    0,
    CASE WHEN vocabulary_id = 'ATC' THEN 1 else 0 END AS ATC_FLAG
FROM @vocabulary_schema.concept
WHERE (concept_class_id = 'Ingredient' AND vocabulary_id = 'RxNorm' AND standard_concept = 'S')
OR (concept_class_id = 'ATC 4th' AND vocabulary_id = 'ATC');
