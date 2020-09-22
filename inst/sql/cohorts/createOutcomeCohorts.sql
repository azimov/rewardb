create table #concept_ancestor_grp as
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
--where ca1.ancestor_concept_id in (4245975, 381270, 4054512)  -- REMOVE FOR FULL RUN
;

-- create clustered columnstore index cci_cag1 on #concept_ancestor_grp;

-- first diagnosis, which eventually leads to hospitalization for same outcome
--HINT DISTRIBUTE_ON_KEY(person_id)
create table #cohorts as
select
  cast(t1.ancestor_concept_id as bigint) * 100 + 1 as cohort_definition_id
  , t1.person_id
  , t1.cohort_start_date
  , t1.cohort_start_date as cohort_end_date
from
(
  select
    co1.person_id
    , ca1.ancestor_concept_id
    , min(co1.condition_start_date) as cohort_start_date
  from @cdm_database_schema.condition_occurrence co1
  inner join #concept_ancestor_grp ca1
    on co1.condition_concept_id = ca1.descendant_concept_id
  group by
    co1.person_id
    , ca1.ancestor_concept_id
) t1
inner join
(
  select
    co1.person_id
    , ca1.ancestor_concept_id
    , min(vo1.visit_start_date) as cohort_start_date
  from @cdm_database_schema.condition_occurrence co1
  inner join @cdm_database_schema.visit_occurrence vo1
    on co1.person_Id = vo1.person_id
    and co1.visit_occurrence_id = vo1.visit_occurrence_id
    and visit_concept_id = 9201
  inner join #concept_ancestor_grp ca1
    on co1.condition_concept_id = ca1.descendant_concept_id
  group by
    co1.person_id
    , ca1.ancestor_concept_id
) t2
  on t1.person_id = t2.person_id
  and t1.ancestor_concept_id = t2.ancestor_concept_id
;

insert into @cohort_database_schema.@outcome_cohort_table
(
  cohort_definition_id
  , subject_id
  , cohort_start_date
  , cohort_end_date
)
select
  cohort_definition_id
  , person_id
  , cohort_start_date
  , cohort_end_date
from #cohorts
;



--incident outcomes - requiring two visits, first visit is used as date of outcome
--HINT DISTRIBUTE_ON_KEY(person_id)
create table #cohortsb as
select
  cast(t1.ancestor_concept_id as bigint) * 100 as cohort_definition_id
  , t1.person_id
  , t1.cohort_start_date
  , t1.cohort_start_date as cohort_end_date
from
(
  select
    co1.person_id
    , ca1.ancestor_concept_id
    , min(co1.condition_start_date) as cohort_start_date
  from @cdm_database_schema.condition_occurrence co1
  inner join #concept_ancestor_grp ca1
    on co1.condition_concept_id = ca1.descendant_concept_id
  group by
    co1.person_id
    , ca1.ancestor_concept_id
) t1
inner join
(
  select
    co1.person_id
    , ca1.ancestor_concept_id
    , min(vo1.visit_start_date) as cohort_start_date
    , max(vo1.visit_start_date) as confirmed_date
  from @cdm_database_schema.condition_occurrence co1
  inner join @cdm_database_schema.visit_occurrence vo1
    on co1.person_Id = vo1.person_id
    and co1.visit_occurrence_id = vo1.visit_occurrence_id
  inner join #concept_ancestor_grp ca1
    on co1.condition_concept_id = ca1.descendant_concept_id
  group by
    co1.person_id
    , ca1.ancestor_concept_id
) t2
  on t1.person_id = t2.person_id
  and t1.ancestor_concept_id = t2.ancestor_concept_id
  where t2.cohort_start_date < t2.confirmed_date -- here's the piece that finds two unique visit dates
;

insert into @cohort_database_schema.@outcome_cohort_table
(
  cohort_definition_id
  , subject_id
  , cohort_start_date
  , cohort_end_date
)
select
  cohort_definition_id
  , person_id
  , cohort_start_date
  , cohort_end_date
from #cohortsb
;

truncate table #concept_ancestor_grp;
drop table #concept_ancestor_grp;

truncate table #cohorts;
drop table #cohorts;

truncate table #cohortsb;
drop table #cohortsb;
