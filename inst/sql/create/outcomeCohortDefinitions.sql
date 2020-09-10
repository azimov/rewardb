create table #cpt_anc_grp as
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

-- Create index on pdw
-- TODO: indexes on other DB platforms
{@dbms == "pdw"} ? {create clustered columnstore index cci_cag1 on #cpt_anc_grp;}

truncate table @cohort_database_schema.@outcome_cohort_definition_table;
--incident outcomes - first diagnosis, which eventually leads to hospitalization for same outcome
INSERT INTO @cohort_database_schema.@outcome_cohort_definition_table
(
  cohort_definition_id
  , cohort_definition_name
  ,	short_name
  , CONCEPTSET_ID
  , outcome_type
)
select
  cast(c1.concept_id as bigint) * 100 + 1 as cohort_definition_id
  , 'Incident outcome of ' + c1.concept_name + ' - first occurence of diagnosis which is observed in hospital in future' as cohort_definition_name
  , 'Incident outcome of ' + c1.concept_name + ' WITH INP' as short_name
  ,	c1.concept_id as CONCEPTSET_ID
  , 1 as outcome_type
from
(
  select
    distinct ca1.ancestor_concept_id
  from @cdm_database_schema.condition_occurrence co1
  inner join @cdm_database_schema.visit_occurrence vo1
    on co1.person_Id = vo1.person_id
    and co1.visit_occurrence_id = vo1.visit_occurrence_id
    and visit_concept_id = 9201
  inner join #cpt_anc_grp ca1
    on co1.condition_concept_id = ca1.descendant_concept_id
) t1

inner join @cdm_database_schema.concept c1
  on t1.ancestor_concept_id = c1.concept_id
;

--outcomes not requiring a hospitalization
INSERT INTO @cohort_database_schema.@outcome_cohort_definition_table
(
  cohort_definition_id
  , cohort_definition_name
  ,	short_name
  , CONCEPTSET_ID
  , outcome_type
)
select
  cast(c1.concept_id as bigint) * 100 as cohort_definition_id
  , 'Incident outcome of ' + c1.concept_name + ' - first occurence of diagnosis' as cohort_definition_name
  , 'Incident outcome of ' + c1.concept_name + ' TWO DX' as short_name
  ,	c1.concept_id as CONCEPTSET_ID
  , 0 as outcome_type
from
(
  select
    distinct ca1.ancestor_concept_id
  from @cdm_database_schema.condition_occurrence co1
  inner join @cdm_database_schema.visit_occurrence vo1
    on co1.person_Id = vo1.person_id
    and co1.visit_occurrence_id = vo1.visit_occurrence_id
  inner join #cpt_anc_grp ca1
    on co1.condition_concept_id = ca1.descendant_concept_id
) t1

inner join @cdm_database_schema.concept c1
  on t1.ancestor_concept_id = c1.concept_id
;

truncate table #cpt_anc_grp;
drop table #cpt_anc_grp;
