{DEFAULT @schema = 'reward'}

DROP SCHEMA IF EXISTS @schema CASCADE;
CREATE SCHEMA @schema;

CREATE TABLE @schema.scc_result (
  source_id INT NOT NULL,
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


CREATE TABLE @schema.data_source (
    source_id INT PRIMARY KEY,
    source_name varchar,
    source_key varchar
);

-- Outcome cohort definitions/Names/Types

-- Exposure cohort definitions/Names

-- Concept mappings

-- Common evidence model for control mappings
DROP SCHEMA IF EXISTS cem CASCADE;
CREATE SCHEMA cem;

CREATE TABLE cem.matrix_summary (
    ingredient_concept_id INT NOT NULL,
    condition_concept_id INT NOT NULL,
    EVIDENCE_EXISTS INT,
    PRIMARY KEY(ingredient_concept_id, condition_concept_id)
);