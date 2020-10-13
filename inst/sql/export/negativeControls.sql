select 
    o.outcome_cohort_id,
    t.target_cohort_id,
    o.cohort_name as outcome_name,
    t.cohort_name as target_name
FROM
    @schema.negative_control nc
INNER JOIN @schema.outcome o ON o.outcome_cohort_id = nc.outcome_cohort_id
INNER JOIN @schema.target t ON t.target_cohort_id = nc.target_cohort_id;