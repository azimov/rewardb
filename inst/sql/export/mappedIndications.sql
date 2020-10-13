select
    o.outcome_cohort_id,
    t.target_cohort_id,
    o.cohort_name as outcome_name,
    t.cohort_name as target_name
FROM
    @schema.positive_indication pi
INNER JOIN @schema.outcome o ON o.outcome_cohort_id = pi.outcome_cohort_id
INNER JOIN @schema.target t ON t.target_cohort_id = pi.target_cohort_id;