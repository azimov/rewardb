SELECT o.outcome_cohort_id, oc.condition_concept_id
FROM outcome o INNER JOIN outcome_concept oc ON o.outcome_cohort_id = oc.outcome_cohort_id;


SELECT * FROM outcome;