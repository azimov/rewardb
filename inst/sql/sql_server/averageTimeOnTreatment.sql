IF OBJECT_ID('tempdb..#risk_windows', 'U') IS NOT NULL
	DROP TABLE #risk_windows;

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
{@exposure_ids != ''} ? { WHERE et.@exposure_id IN (@exposure_ids) }
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

SELECT
    exposure_id as target_cohort_id,
    outcome_id as outcome_cohort_id,
    avg(time_at_risk_exposed) AS mean_tx_time,
    stdev(time_at_risk_exposed) AS sd_tx_time

FROM (
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
  	    DATEDIFF(DAY, risk_window_start_exposed, risk_window_end_exposed) + 1 AS time_at_risk_exposed
    FROM #risk_windows risk_windows
    INNER JOIN (
        SELECT person_id,
            outcome_id,
            outcome_date
        FROM (
            SELECT subject_id AS person_id,
                ot.@outcome_id AS outcome_id,
                ot.@outcome_start_date AS outcome_date,
                ROW_NUMBER() OVER (PARTITION BY ot.@outcome_person_id, ot.@outcome_id  ORDER BY ot.@outcome_start_date) AS rn1
            FROM
                @outcome_database_schema.@outcome_table ot
            {@outcome_ids != ''} ? { WHERE ot.@outcome_id IN (@outcome_ids) }
        ) raw_outcomes
  	    WHERE rn1 = 1
    ) outcomes
    ON risk_windows.person_id = outcomes.person_id
) q WHERE q.is_exposed_outcome = 1 OR q.is_unexposed_outcome = 1
GROUP BY exposure_id, outcome_id;