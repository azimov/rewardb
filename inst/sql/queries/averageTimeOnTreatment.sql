with drug_outcome as (
    select a.cohort_definition_id as cohort_id,
           b.cohort_definition_id as outcome_cohort_id,
           b.subject_id,
           b.cohort_start_date as outcome_date,
           a.cohort_definition_id,
           a.cohort_start_date as treatment_start,
           a.cohort_end_date as treatment_end,
           datediff(day, a.cohort_start_date, a.cohort_end_date) + 1 as time_on_treatment,
           abs(datediff(day, b.cohort_start_date, a.cohort_start_date))AS time_to_outcome
    from @schema.@cohort_table a
    inner join @schema.@outcome_cohort_table b on a.subject_id = b.subject_id

    where (
         -- Outcome starts after treatment and outcome ends on or before treatment end
        (b.cohort_start_date > a.cohort_start_date AND b.cohort_start_date <= a.cohort_end_date) OR
        -- Outcome starts before treatment starts AND outcome ends on or after end of time on treatment
        (b.cohort_start_date < a.cohort_start_date
            AND b.cohort_start_date >= dateadd(day, -1 * (datediff(day, a.cohort_start_date, a.cohort_end_date) + 1), a.cohort_start_date))
    )
    {@subset_outcomes} ? {AND b.cohort_definition_id IN (@outcome_cohort_ids)}
    {@subset_targets} ? {AND a.cohort_definition_id IN (@target_cohort_ids)}
),
obs as (
    select
        b.*,
        a.observation_period_start_date,
        a.observation_period_end_date,
        datediff(day, a.observation_period_start_date, b.treatment_start) as prior_obs_length,
           CASE
                WHEN datediff(day, a.observation_period_start_date, b.treatment_start) >= b.time_on_treatment THEN 1
                ELSE 0
           END AS sufficient_obs_time
    from @cdm_schema.observation_period a
    inner join drug_outcome b on (
        a.person_id = b.subject_id
        AND a.observation_period_start_date <= b.treatment_start
        AND a.observation_period_end_date >= b.treatment_start
    )
)

select
    cohort_id,
    outcome_cohort_id,
    count(subject_id) as outcome_count,
    avg(time_to_outcome) as mean_time_to_outcome,
    avg(time_on_treatment) as mean_tx_time,
    stdev(time_on_treatment) as sd_tx_time,
    sufficient_obs_time
from obs
where sufficient_obs_time = 1
group by sufficient_obs_time, cohort_id, outcome_cohort_id
