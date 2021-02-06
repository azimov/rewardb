IF OBJECT_ID('tempdb..#risk_windows', 'U') IS NOT NULL
	DROP TABLE #risk_windows;

IF OBJECT_ID('tempdb..#results', 'U') IS NOT NULL
	DROP TABLE #results;

-- Create risk windows
--HINT DISTRIBUTE_ON_KEY(person_id)
SELECT
    person_id,
    exposure_id,
    risk_window_start_exposed,
    risk_window_end_exposed,
    risk_window_start_unexposed,
    risk_window_end_unexposed
INTO #risk_windows
FROM (
	SELECT t1.person_id,
		exposure_id,
		CASE WHEN
			DATEADD(DAY, @risk_window_start_exposed, exposure_start_date) <= op1.observation_period_end_date
		THEN
			DATEADD(DAY, @risk_window_start_exposed, exposure_start_date)
	    ELSE
			op1.observation_period_end_date
		END AS risk_window_start_exposed,
		CASE WHEN
			DATEADD(DAY, {@add_length_of_exposure_exposed} ? {DATEDIFF(DAY, exposure_start_date, exposure_end_date)} : {0} + @risk_window_end_exposed, exposure_start_date) <= op1.observation_period_end_date
		THEN
			DATEADD(DAY, {@add_length_of_exposure_exposed} ? {DATEDIFF(DAY, exposure_start_date, exposure_end_date)} : {0} + @risk_window_end_exposed, exposure_start_date)
	    ELSE
			op1.observation_period_end_date
		END AS risk_window_end_exposed,
		CASE WHEN
			DATEADD(DAY, {@add_length_of_exposure_unexposed} ? {-DATEDIFF(DAY, exposure_start_date, exposure_end_date)} : {0} + @risk_window_start_unexposed, exposure_start_date) >= op1.observation_period_start_date
		THEN
			DATEADD(DAY, {@add_length_of_exposure_unexposed} ? {-DATEDIFF(DAY, exposure_start_date, exposure_end_date)} : {0} + @risk_window_start_unexposed, exposure_start_date)
	    ELSE
			op1.observation_period_start_date
		END AS risk_window_start_unexposed,
		CASE WHEN
			DATEADD(DAY, @risk_window_end_unexposed, exposure_start_date) >= op1.observation_period_start_date
		THEN
			DATEADD(DAY, @risk_window_end_unexposed, exposure_start_date)
	    ELSE
			op1.observation_period_start_date
		END AS risk_window_end_unexposed
	FROM (
		SELECT person_id,
			exposure_id,
			exposure_start_date,
			exposure_end_date
		FROM (
			SELECT et.@exposure_person_id AS person_id,
				et.@exposure_id AS exposure_id,
				et.@exposure_start_date AS exposure_start_date,
				et.@exposure_end_date AS exposure_end_date
				{@first_exposure_only} ? {,ROW_NUMBER() OVER (PARTITION BY et.@exposure_person_id, et.@exposure_id ORDER BY et.@exposure_start_date) AS rn1}
			FROM
				@exposure_database_schema.@exposure_table et
{@exposure_ids != ''} ? {			INNER JOIN #scc_exposure_id sei ON sei.exposure_id = et.@exposure_id }
		) raw_exposures
{@first_exposure_only} ? {		WHERE rn1 = 1}
	) t1
	INNER JOIN @cdm_database_schema.person p1
		ON t1.person_id = p1.person_id
	INNER JOIN @cdm_database_schema.observation_period op1
		ON t1.person_id = op1.person_id
	WHERE t1.exposure_start_date >= op1.observation_period_start_date
		AND t1.exposure_start_date <= op1.observation_period_end_date
{@study_start_date != ''} ? {		AND exposure_start_date >= CONVERT(DATE, '@study_start_date')}
{@study_end_date != ''} ? {		AND exposure_start_date <= CONVERT(DATE, '@study_end_date')}
{@min_age != ''} ? {		AND YEAR(exposure_start_date) - p1.year_of_birth  >= @min_age}
{@max_age != ''} ? {		AND YEAR(exposure_start_date) - p1.year_of_birth  <= @max_age}
{@has_full_time_at_risk}  ? {
		AND op1.observation_period_end_date >= DATEADD(DAY, {@add_length_of_exposure_exposed} ? {DATEDIFF(DAY, exposure_start_date, exposure_end_date)} : {0} + @risk_window_end_exposed, exposure_start_date)
		AND op1.observation_period_start_date <= DATEADD(DAY, -{@add_length_of_exposure_exposed} ? {DATEDIFF(DAY, exposure_start_date, exposure_end_date)} : {0} + @risk_window_start_unexposed, exposure_start_date)
}
{@washout_window != ''} ? {		AND DATEDIFF(DAY, op1.observation_period_start_date, exposure_start_date) >= @washout_window}
{@followup_window != ''} ? {		AND DATEDIFF(DAY, exposure_start_date, op1.observation_period_end_date) >= @followup_window}
) t2
WHERE risk_window_end_exposed >= risk_window_start_exposed
	AND risk_window_end_unexposed >= risk_window_start_unexposed;

WITH treatment_times as (
    SELECT
        exposure_id,
        outcome_id,
        CASE WHEN outcome_date >= risk_window_start_exposed AND outcome_date <= risk_window_end_exposed
            THEN 1
            ELSE 0
        END AS is_exposed_outcome,

        CASE WHEN outcome_date >= risk_window_start_unexposed AND outcome_date <= risk_window_end_unexposed
            THEN 1
            ELSE 0
        END AS is_unexposed_outcome,

  	    DATEDIFF(DAY, risk_window_start_exposed, risk_window_end_exposed) + 1 AS time_at_risk_exposed,
  	    abs(DATEDIFF(DAY, risk_window_start_exposed, outcome_date) + 1) AS time_to_outcome

    FROM #risk_windows risk_windows
    INNER JOIN (
        SELECT person_id,
            outcome_id,
            outcome_date
        FROM (
            SELECT subject_id AS person_id,
                ot.@outcome_id AS outcome_id,
                ot.@outcome_start_date AS outcome_date
                {@first_outcome_only} ? {,ROW_NUMBER() OVER (PARTITION BY ot.@outcome_person_id, ot.@outcome_id ORDER BY ot.@outcome_start_date) AS rn1}
            FROM
                @outcome_database_schema.@outcome_table ot
{@outcome_ids != ''} ? {		INNER JOIN #scc_outcome_ids soi ON soi.outcome_id = ot.@outcome_id}
        ) raw_outcomes
  	    {@first_outcome_only} ? {	WHERE rn1 = 1}
    ) outcomes
    ON risk_windows.person_id = outcomes.person_id
),
tx_distribution as (
       select
              o.exposure_id,
              o.outcome_id,
              o.mean_tx_time,
              coalesce(o.sd_tx_time, 0) as sd_tx_time,
              o.min_tx_time,
              MIN(case when s.accumulated >= .10 * o.total then time_at_risk_exposed else o.max_tx_time end) as p10_tx_time,
              MIN(case when s.accumulated >= .25 * o.total then time_at_risk_exposed else o.max_tx_time end) as p25_tx_time,
              MIN(case when s.accumulated >= .50 * o.total then time_at_risk_exposed else o.max_tx_time end) as median_tx_time,
              MIN(case when s.accumulated >= .75 * o.total then time_at_risk_exposed else o.max_tx_time end) as p75_tx_time,
              MIN(case when s.accumulated >= .90 * o.total then time_at_risk_exposed else o.max_tx_time end) as p90_tx_time,
              o.max_tx_time
       FROM (
              select
                     exposure_id,
                     outcome_id,
                     avg(1.0 * time_at_risk_exposed) as mean_tx_time,
                     stdev(time_at_risk_exposed) as sd_tx_time,
                     min(time_at_risk_exposed) as min_tx_time,
                     max(time_at_risk_exposed) as max_tx_time,
                     count_big(*) as total
              from treatment_times q
              WHERE q.is_exposed_outcome = 1 OR q.is_unexposed_outcome = 1
              group by exposure_id, outcome_id
       ) o
       join (
              select exposure_id, outcome_id, time_at_risk_exposed, count_big(*) as total,
                     sum(count_big(*)) over (partition by exposure_id, outcome_id order by time_at_risk_exposed) as accumulated
              from treatment_times q
              WHERE q.is_exposed_outcome = 1 OR q.is_unexposed_outcome = 1
              group by exposure_id, outcome_id, time_at_risk_exposed
       ) s on (o.exposure_id = s.exposure_id and o.outcome_id = s.outcome_id)
       group by o.exposure_id, o.outcome_id, o.total, o.min_tx_time, o.max_tx_time, o.mean_tx_time, o.sd_tx_time
),

time_to_distribution as (
       select
              o.exposure_id,
              o.outcome_id,
              o.mean_time_to_outcome,
              coalesce(o.sd_time_to_outcome, 0) as sd_time_to_outcome,
              o.min_time_to_outcome,
              MIN(case when s.accumulated >= .10 * o.total then time_to_outcome else o.max_time_to_outcome end) as p10_time_to_outcome,
              MIN(case when s.accumulated >= .25 * o.total then time_to_outcome else o.max_time_to_outcome end) as p25_time_to_outcome,
              MIN(case when s.accumulated >= .50 * o.total then time_to_outcome else o.max_time_to_outcome end) as median_time_to_outcome,
              MIN(case when s.accumulated >= .75 * o.total then time_to_outcome else o.max_time_to_outcome end) as p75_time_to_outcome,
              MIN(case when s.accumulated >= .90 * o.total then time_to_outcome else o.max_time_to_outcome end) as p90_time_to_outcome,
              o.max_time_to_outcome
       FROM (
              select
                     exposure_id,
                     outcome_id,
                     avg(1.0 * time_to_outcome) as mean_time_to_outcome,
                     stdev(time_to_outcome) as sd_time_to_outcome,
                     min(time_to_outcome) as min_time_to_outcome,
                     max(time_to_outcome) as max_time_to_outcome,
                     count_big(*) as total
              from treatment_times q
              WHERE q.is_exposed_outcome = 1 OR q.is_unexposed_outcome = 1
              group by exposure_id, outcome_id
       ) o
       join (
              select exposure_id, outcome_id, time_to_outcome, count_big(*) as total,
                     sum(count_big(*)) over (partition by exposure_id, outcome_id order by time_to_outcome) as accumulated
              from treatment_times q
              WHERE q.is_exposed_outcome = 1 OR q.is_unexposed_outcome = 1
              group by exposure_id, outcome_id, time_to_outcome
       ) s on (o.exposure_id = s.exposure_id and o.outcome_id = s.outcome_id)
       group by o.exposure_id, o.outcome_id, o.total, o.min_time_to_outcome, o.max_time_to_outcome, o.mean_time_to_outcome, o.sd_time_to_outcome
)

select tx.*,
    tt.mean_time_to_outcome,
    tt.sd_time_to_outcome,
    tt.min_time_to_outcome,
    tt.p10_time_to_outcome,
    tt.p25_time_to_outcome,
    tt.median_time_to_outcome,
    tt.p75_time_to_outcome,
    tt.p90_time_to_outcome,
    tt.max_time_to_outcome
    INTO #results
    from tx_distribution tx
    INNER JOIN time_to_distribution tt ON (tt.exposure_id = tx.exposure_id AND tt.outcome_id = tx.outcome_id);
--- Note: if you add paritioning keys, you must join s on o with those keys, and the SUM window function for accumulated must be partitioned the same way

TRUNCATE TABLE #risk_windows;
DROP TABLE #risk_windows;

{@outcome_ids != ''} ? {
TRUNCATE TABLE #scc_outcome_ids;
DROP TABLE #scc_outcome_ids;
}
{@exposure_ids != ''} ? {
TRUNCATE TABLE #scc_exposure_ids;
DROP TABLE #scc_exposure_ids;
}