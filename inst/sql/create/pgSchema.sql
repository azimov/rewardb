{DEFAULT @schema = 'reward'}

DROP SCHEMA IF EXISTS @schema CASCADE;
CREATE SCHEMA @schema;

CREATE TABLE @schema.scc_result (
  source_id INT NOT NULL,
  analysis_id INT NOT NULL,
  outcome_cohort_id BIGINT NOT NULL,
  target_cohort_id BIGINT NOT NULL,
  rr NUMERIC,
  se_log_rr NUMERIC,
  c_pt NUMERIC,
  t_pt NUMERIC,
  t_at_risk NUMERIC,
  c_at_risk NUMERIC,
  t_cases NUMERIC,
  c_cases NUMERIC,
  lb_95 NUMERIC,
  ub_95 NUMERIC,
  p_value NUMERIC,
  I2 NUMERIC
);

CREATE TABLE @schema.time_on_treatment (
    source_id INT NOT NULL,
    analysis_id INT,
    outcome_cohort_id BIGINT NOT NULL,
    target_cohort_id BIGINT NOT NULL,
    mean_time_to_outcome NUMERIC,
    sd_time_to_outcome NUMERIC,
    min_time_to_outcome NUMERIC,
    p10_time_to_outcome NUMERIC,
    p25_time_to_outcome NUMERIC,
    median_time_to_outcome NUMERIC,
    p75_time_to_outcome NUMERIC,
    p90_time_to_outcome NUMERIC,
    max_time_to_outcome NUMERIC,
    mean_tx_time NUMERIC,
    sd_tx_time NUMERIC,
    min_tx_time NUMERIC,
    p10_tx_time NUMERIC,
    p25_tx_time NUMERIC,
    median_tx_time NUMERIC,
    p75_tx_time NUMERIC,
    p90_tx_time NUMERIC,
    max_tx_time NUMERIC
);


CREATE TABLE @schema.data_source (
    source_id INT PRIMARY KEY,
    source_name varchar,
    source_key varchar
);
