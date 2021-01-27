{@delete_existing} ? {
if object_id('@cohort_database_schema.@outcome_cohort_table', 'U') is not null
	drop table @cohort_database_schema.@outcome_cohort_table;
}

--HINT DISTRIBUTE_ON_KEY(subject_id)
if object_id('@cohort_database_schema.@outcome_cohort_table', 'U') is null
    create table @cohort_database_schema.@outcome_cohort_table
    (
        cohort_definition_id bigint
        , subject_id bigint
        ,	cohort_start_date date
        ,	cohort_end_date date
    );


IF OBJECT_ID('@cohort_database_schema.computed_o_cohorts', 'U') IS NOT NULL
	DROP TABLE @cohort_database_schema.computed_o_cohorts;

IF OBJECT_ID('@cohort_database_schema.concept_ancestor_grp', 'U') IS NOT NULL
	DROP TABLE @cohort_database_schema.concept_ancestor_grp;

--HINT DISTRIBUTE_ON_KEY(cohort_definition_id)
create table @cohort_database_schema.computed_o_cohorts AS
SELECT DISTINCT cohort_definition_id
FROM @cohort_database_schema.@outcome_cohort_table;

create table @cohort_database_schema.concept_ancestor_grp as
select
  ca1.ancestor_concept_id
  , ca1.descendant_concept_id
from @cdm_database_schema.concept_ancestor ca1
inner join
(
  select
    c1.concept_id
    , c1.concept_name
    , c1.vocabulary_id
    , c1.domain_id
  from @cdm_database_schema.concept c1
  inner join @cdm_database_schema.concept_ancestor ca1
    on ca1.ancestor_concept_id = 441840 -- clinical finding
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



