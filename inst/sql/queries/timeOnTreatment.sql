with outcome as (
select *
from @schema.@outcome_cohort_table
where cohort_definition_id = @outcome_cohort_id
),
drug as (
select cohort_definition_id, subject_id, cohort_start_date as treatment_start, cohort_end_date as treatment_end,
       datediff(day, cohort_start_date, cohort_end_date) + 1 as time_on_treatment
from @schema.@cohort_table
where cohort_definition_id = @cohort_id
),
drug_outcome as (
select b.cohort_definition_id as outcome_cohort_id, b.subject_id, b.cohort_start_date as outcome_date,
       a.treatment_start, a.treatment_end, a.time_on_treatment,
	   -- do case statement for if outcome comes before or after treatment_start
	   CASE
		WHEN b.cohort_start_date < a.treatment_start THEN datediff(day, b.cohort_start_date, a.treatment_start)
		WHEN b.cohort_start_date > a.treatment_start THEN datediff(day, a.treatment_start, b.cohort_start_date)
		ELSE 0
	   END AS time_to_outcome
from drug a
inner join outcome b
on a.subject_id = b.subject_id
where (b.cohort_start_date > a.treatment_start AND b.cohort_start_date <= a.treatment_end) OR
      (b.cohort_start_date < a.treatment_start AND b.cohort_start_date >= dateadd(day, -1*a.time_on_treatment, a.treatment_start)) -- QC this code
),
obs as (
select b.*, a.observation_period_start_date, a.observation_period_end_date,
       datediff(day, a.observation_period_start_date, b.treatment_start) as prior_obs_length,
       @cohort_id as cohort_id,
       case when datediff(day, a.observation_period_start_date, b.treatment_start) >= b.time_on_treatment THEN 1
       ELSE 0
       END AS sufficient_obs_time
from @cdm_schema.observation_period a
inner join drug_outcome b
on a.person_id = b.subject_id
   AND a.observation_period_start_date <= b.treatment_start
   AND a.observation_period_end_date >= b.treatment_start
),
drug_count as (
select @cohort_id as cohort_id, @outcome_cohort_id as outcome_cohort_id, count(a.subject_id) as drug_count
from drug a
inner join @cdm_schema.observation_period b
on a.subject_id = b.person_id
   AND b.observation_period_start_date <= a.treatment_start
   AND b.observation_period_end_date >= a.treatment_start
   AND datediff(day, b.observation_period_start_date, a.treatment_start) >= a.time_on_treatment
),
outcome_cnt as (
select @cohort_id as cohort_id, count(*) as outcome_cnt
from obs
where sufficient_obs_time = 1
),
time_variables as (
select @cohort_id as cohort_id, avg(time_to_outcome) as mean_time_outcome, avg(time_on_treatment) as mean_tx_time, stdev(time_on_treatment) as sd_tx_time, sufficient_obs_time
from obs
where sufficient_obs_time = 1
group by sufficient_obs_time
),
merged as (
select a.*, b.outcome_cnt, c.mean_time_outcome, c.mean_tx_time, c.sd_tx_time, c.sufficient_obs_time
from drug_count a
inner join outcome_cnt b
on a.cohort_id = b.cohort_id
inner join time_variables c
on a.cohort_id = c.cohort_id
)
select cohort_id, outcome_cohort_id, drug_count, outcome_cnt, mean_time_outcome, mean_tx_time, sd_tx_time
from merged
group by cohort_id, outcome_cohort_id, drug_count, outcome_cnt, mean_time_outcome, mean_tx_time, sd_tx_time;


