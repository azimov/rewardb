if object_id('tempdb..#incident_outcomes', 'U') is not null
	drop table #incident_outcomes
;

--HINT DISTRIBUTE_ON_KEY(subject_id)
create table #incident_outcomes as
select
	hoc1.cohort_definition_id
	, hoc1.subject_id
	, hoc1.cohort_start_date
	, hoc1.cohort_end_date
from @cohort_database_schema.@outcome_cohort_table hoc1
inner join @cohort_database_schema.@outcome_cohort_definition_table hocd1
	on hoc1.cohort_definition_id = hocd1.cohort_definition_id
{@use_custom_outcome_cohort_ids} ? {WHERE hoc1.cohort_definition_id IN (@custom_outcome_cohort_ids)}
;

if object_id('tempdb..#drug_summary', 'U') is not null
	drop table #drug_summary;

--HINT DISTRIBUTE_ON_KEY(target_cohort_definition_id)
create table #drug_summary as
select
	c1.cohort_definition_id as target_cohort_definition_id
	, count(c1.subject_id) as num_persons
	, sum(datediff(dd,c1.cohort_start_date, c1.cohort_end_date) / 365.25) as pt_pp_post
	, sum(datediff(dd,c1.cohort_start_date, op1.observation_period_end_date) / 365.25) as pt_itt_post
from @cohort_database_schema.@cohort_table c1
inner join @cdm_database_schema.observation_period op1
	on c1.subject_id = op1.person_id
	and c1.cohort_start_date >= op1.observation_period_start_date
	and c1.cohort_start_date <= op1.observation_period_end_date
group by
	c1.cohort_definition_id
;


if object_id('tempdb..#drug_outcome_count', 'U') is not null
	drop table #drug_outcome_count
;

--HINT DISTRIBUTE_ON_KEY(target_cohort_definition_id)
create table #drug_outcome_count as
select
	c1.cohort_definition_id as target_cohort_definition_id
	, io1.cohort_definition_id as outcome_cohort_definition_id
	, sum(case when io1.cohort_start_date <= c1.cohort_start_date then 1 else 0 end) as num_persons_prior_outcome
	, sum(case when io1.cohort_start_date <= c1.cohort_start_date then datediff(dd, c1.cohort_start_date,op1.observation_period_end_date) else 0 end / 365.25) as pt_itt_censor_prior
	, sum(case when io1.cohort_start_date <= c1.cohort_start_date then datediff(dd, c1.cohort_start_date,c1.cohort_end_date) else 0 end / 365.25) as pt_pp_censor_prior
	, sum(case when io1.cohort_start_date > c1.cohort_start_date and io1.cohort_start_date <= op1.observation_period_end_date then 1 else 0 end) as num_persons_post_itt
	, sum(case when io1.cohort_start_date > c1.cohort_start_date and io1.cohort_start_date <= op1.observation_period_end_date then datediff(dd, io1.cohort_start_date,op1.observation_period_end_date) else 0 end / 365.25) as pt_itt_censor_post
	, sum(case when io1.cohort_start_date > c1.cohort_start_date and io1.cohort_start_date <= c1.cohort_end_date then 1 else 0 end) as num_persons_post_pp
	, sum(case when io1.cohort_start_date > c1.cohort_start_date and io1.cohort_start_date <= c1.cohort_end_date then datediff(dd, io1.cohort_start_date,c1.cohort_end_date) else 0 end / 365.25) as pt_pp_censor_post
from @cohort_database_schema.@cohort_table c1
inner join @cdm_database_schema.observation_period op1
	on c1.subject_id = op1.person_id
	and c1.cohort_start_date >= op1.observation_period_start_date
	and c1.cohort_start_date <= op1.observation_period_end_date
inner join #incident_outcomes io1
	on c1.subject_id = io1.subject_id
group by
	c1.cohort_definition_id
	, io1.cohort_definition_id
;

if object_id('tempdb..#drug_outcome_summary', 'U') is not null
	drop table #drug_outcome_summary
;

--HINT DISTRIBUTE_ON_KEY(target_cohort_definition_id)
create table #drug_outcome_summary as
select
	do1.target_cohort_definition_id
	, do1.outcome_cohort_definition_id
	, d1.num_persons - do1.num_persons_prior_outcome as num_persons_pp_risk
	, do1.num_persons_post_pp
	, d1.pt_pp_post - do1.pt_pp_censor_prior - do1.pt_pp_censor_post as pt_pp
	, do1.num_persons_post_itt
	, d1.pt_itt_post - do1.pt_itt_censor_prior - do1.pt_itt_censor_post as pt_itt
from #drug_summary d1
inner join #drug_outcome_count do1
	on d1.target_cohort_definition_id = do1.target_cohort_definition_id;

INSERT INTO @cohort_database_schema.@outcome_summary_table (
   target_cohort_definition_id,
   outcome_cohort_definition_id,
   at_risk_pp,
   cases_pp,
   pt_pp,
   ip_pp,
   ir_pp,
   cases_itt,
   pt_itt,
   ip_itt,
   ir_itt
)
select
	dos1.target_cohort_definition_id
	, dos1.outcome_cohort_definition_id
	, dos1.num_persons_pp_risk as at_risk_pp
	, dos1.num_persons_post_pp as cases_pp
	, dos1.pt_pp
	, case when dos1.num_persons_pp_risk > 0 then 1.0 * dos1.num_persons_post_pp / dos1.num_persons_pp_risk else null end as ip_pp
	, case when dos1.pt_pp > 0 then 1.0 * dos1.num_persons_post_pp / dos1.pt_pp else null end as ir_pp
	, dos1.num_persons_post_itt as cases_itt
	, dos1.pt_itt
	, case when dos1.num_persons_pp_risk > 0 then 1.0 * dos1.num_persons_post_itt / dos1.num_persons_pp_risk else null end as ip_itt
	, case when dos1.pt_itt > 0 then 1.0 * dos1.num_persons_post_itt / dos1.pt_itt else null end as ir_itt
from #drug_outcome_summary dos1;

if object_id('tempdb..#concept_ancestor_grp', 'U') is not null
	drop table #concept_ancestor_grp
;

if object_id('tempdb..#incident_outcomes', 'U') is not null
	drop table #incident_outcomes
;

if object_id('tempdb..#drug_outcome_summary', 'U') is not null
	drop table #drug_outcome_summary
;
