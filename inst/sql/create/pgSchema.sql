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
	concept_name varchar(1000),
	concept_id bigint,
	is_excluded INT,
	include_Descendants INT,
	include_Mapped INT
)
;

/* COHORT DEFINITION TABLE */
create table @schema.cohort_definition
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
create table @schema.outcome_cohort_definition
(
	cohort_definition_id SERIAL PRIMARY KEY
	, cohort_definition_name varchar(1000)
	,	short_name varchar(1000)
	, CONCEPTSET_ID bigint
	, outcome_type int /*0-outcomes for 2+ diagnoses, 1-hospitalizations, 2-custom outcome cohort*/
)
;


CREATE TABLE @schema.atlas_outcome_reference (
    COHORT_DEFINITION_ID BIGINT,
    ATLAS_ID BIGINT,
    sql_definition text,
    DEFINITION text, -- Stores sql used to generate the cohort
    atlas_url varchar(1000), -- Base atlas url used to pull cohort
    CONSTRAINT cohort_def
      FOREIGN KEY(COHORT_DEFINITION_ID)
	    REFERENCES @schema.outcome_cohort_definition(COHORT_DEFINITION_ID)
	        ON DELETE CASCADE

);

CREATE TABLE @schema.atlas_concept_reference (
    COHORT_DEFINITION_ID INT,
    CONCEPT_ID BIGINT,
    INCLUDE_DESCENDANTS INT,
    IS_EXCLUDED INT,
    INCLUDE_MAPPED INT,
    CONSTRAINT cohort_def
      FOREIGN KEY(COHORT_DEFINITION_ID)
	    REFERENCES @schema.outcome_cohort_definition(COHORT_DEFINITION_ID)
	        ON DELETE CASCADE
);


CREATE TABLE @schema.custom_exposure (
    COHORT_DEFINITION_ID BIGINT,
    CONCEPT_SET_ID INT,
    ATLAS_URL varchar(1000),
    CONSTRAINT cohort_def
      FOREIGN KEY(COHORT_DEFINITION_ID)
	    REFERENCES @schema.cohort_definition(COHORT_DEFINITION_ID)
	        ON DELETE CASCADE
);

CREATE TABLE @schema.custom_exposure_concept (
    COHORT_DEFINITION_ID BIGINT,
    CONCEPT_ID BIGINT,
    INCLUDE_DESCENDANTS INT,
    IS_EXCLUDED INT,
    INCLUDE_MAPPED INT,
    CONSTRAINT cohort_def
      FOREIGN KEY(COHORT_DEFINITION_ID)
	    REFERENCES @schema.cohort_definition(COHORT_DEFINITION_ID)
	        ON DELETE CASCADE
);

-- Create ingredient cohorts from vocabulary

DELETE FROM @schema.concept_set_definition;
INSERT INTO @schema.concept_set_definition (
    CONCEPT_ID,
    CONCEPT_NAME,
    CONCEPTSET_ID,
    IS_EXCLUDED,
    INCLUDE_DESCENDANTS,
    INCLUDE_MAPPED
)
SELECT DISTINCT concept_id , concept_name, concept_id, 0, 1, 0
    from @vocabulary_schema.concept
    where (concept_class_id = 'Ingredient' AND vocabulary_id = 'RxNorm' AND standard_concept = 'S')
       OR (concept_class_id = 'ATC 4th' AND vocabulary_id = 'ATC');

INSERT INTO @schema.cohort_definition (
    COHORT_DEFINITION_NAME,
    SHORT_NAME,
    DRUG_CONCEPTSET_ID,
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
    CASE WHEN vocabulary_id = 'ATC' THEN 1 else 0 END AS ATC_FLAG
FROM @vocabulary_schema.concept
WHERE (concept_class_id = 'Ingredient' AND vocabulary_id = 'RxNorm' AND standard_concept = 'S')
OR (concept_class_id = 'ATC 4th' AND vocabulary_id = 'ATC');


/* OUTCOME DEFINITIONS */
-- Create outcome cohort definitions
create temporary table #cpt_anc_grp as
select
  ca1.ancestor_concept_id
  , ca1.descendant_concept_id
from @vocabulary_schema.concept_ancestor ca1
inner join
(
  select
    c1.concept_id
    , c1.concept_name
    , c1.vocabulary_id
    , c1.domain_id
  from @vocabulary_schema.concept c1
  inner join @vocabulary_schema.concept_ancestor ca1
    on ca1.ancestor_concept_id = 441840 /* clinical finding */
    and c1.concept_id = ca1.descendant_concept_id
  where
  (
    ca1.min_levels_of_separation > 2
  	or c1.concept_id in (433736, 433595, 441408, 72404, 192671, 137977, 434621, 437312, 439847, 4171917, 438555, 4299449, 375258, 76784, 40483532, 4145627, 434157, 433778, 258449, 313878)
  )
  and c1.concept_name not like '%finding'
  and c1.concept_name not like 'disorder of%'
  and c1.concept_name not like 'finding of%'
  and c1.concept_name not like 'disease of%'
  and c1.concept_name not like 'injury of%'
  and c1.concept_name not like '%by site'
  and c1.concept_name not like '%by body site'
  and c1.concept_name not like '%by mechanism'
  and c1.concept_name not like '%of body region'
  and c1.concept_name not like '%of anatomical site'
  and c1.concept_name not like '%of specific body structure%'
  and c1.domain_id = 'Condition'
) t1
  on ca1.ancestor_concept_id = t1.concept_id
;

--incident outcomes - first diagnosis, which eventually leads to hospitalization for same outcome
INSERT INTO @schema.outcome_cohort_definition
(
  cohort_definition_name
  ,	short_name
  , CONCEPTSET_ID
  , outcome_type
)
select
    'Incident outcome of ' + c1.concept_name + ' - first occurence of diagnosis which is observed in hospital in future' as cohort_definition_name
  , 'Incident outcome of ' + c1.concept_name + ' WITH INP' as short_name
  ,	c1.concept_id as CONCEPTSET_ID
  , 1 as outcome_type
from
#cpt_anc_grp ca1
inner join @vocabulary_schema.concept c1
  on ca1.ancestor_concept_id = c1.concept_id
;

--outcomes not requiring a hospitalization
INSERT INTO @schema.outcome_cohort_definition
(
  cohort_definition_name
  ,	short_name
  , CONCEPTSET_ID
  , outcome_type
)
select
  'Incident outcome of ' + c1.concept_name + ' - first occurence of diagnosis' as cohort_definition_name
  , 'Incident outcome of ' + c1.concept_name + ' TWO DX' as short_name
  ,	c1.concept_id as CONCEPTSET_ID
  , 0 as outcome_type
from
#cpt_anc_grp ca1
inner join @vocabulary_schema.concept c1
  on ca1.ancestor_concept_id = c1.concept_id
;

truncate table #cpt_anc_grp;
drop table #cpt_anc_grp;
