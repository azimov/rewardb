-- REWARD-B dataset. Each app has its own dataset.

DROP TABLE IF EXISTS @schema.result;
CREATE TABLE @schema.result (
    source_id INT NOT NULL,
    analysis_id INT,
    outcome_cohort_id BIGINT NOT NULL,
    target_cohort_id BIGINT NOT NULL,
    calibrated INT NOT NULL DEFAULT 0, -- is the result calibrated with empirical calibration?
    study_design VARCHAR, -- SCC or SCCS
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
    I2 NUMERIC,
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

CREATE INDEX result_outcome_cohort_id_idx ON @schema.result (outcome_cohort_id,target_cohort_id);


DROP TABLE IF EXISTS @schema.target;
-- Maps to CDM drug concept. Only one drug concept may be associated with a target cohort
CREATE TABLE @schema.target (
    target_cohort_id BIGINT NOT NULL PRIMARY KEY,
    cohort_name VARCHAR,
    is_atc_4 INT,
    is_custom_cohort INT
);

CREATE TABLE @schema.target_concept (
    target_cohort_id BIGINT NOT NULL,
    concept_id INT NOT NULL,
    is_excluded INT,
    include_descendants INT
);

CREATE TABLE @schema.target_exposure_class (
    target_cohort_id BIGINT NOT NULL,
    exposure_class_id BIGINT NOT NULL
);

CREATE TABLE @schema.exposure_class (
    exposure_class_id BIGINT NOT NULL,
    exposure_class_name VARCHAR,
    exposure_class_type INT DEFAULT 0 -- Will be used when there are multiple meta data types
);

DROP TABLE IF EXISTS @schema.outcome;
-- Maps outcomes to names and outcome types.
CREATE TABLE @schema.outcome (
    outcome_cohort_id BIGINT,
    type_id INT,
    cohort_name VARCHAR
);

DROP TABLE IF EXISTS @schema.outcome_concept;
-- Maps to CDM condition concept. Many condition concepts may be associated with an outcome cohort
CREATE TABLE @schema.outcome_concept (
   outcome_cohort_id BIGINT NOT NULL,
   condition_concept_id BIGINT NOT NULL,
   include_descendants INT DEFAULT 1,
   is_excluded INT DEFAULT 0,
   PRIMARY KEY(outcome_cohort_id, condition_concept_id)
);

DROP TABLE IF EXISTS @schema.cohort_type;
CREATE TABLE @schema.cohort_type (
    cohort_type_id INT PRIMARY KEY,
    description varchar -- e.g. Inpatient diagnosis, two diagnosis codes, ATLAS,
);

INSERT INTO @schema.cohort_type (cohort_type_id, description) values (0, 'Incident of outcome with inpatient visit');
INSERT INTO @schema.cohort_type (cohort_type_id, description) values (1, 'Incident of outcome with two diagnosis codes');
INSERT INTO @schema.cohort_type (cohort_type_id, description) values (2, 'Incident of outcome with one diagnosis codes');
INSERT INTO @schema.cohort_type (cohort_type_id, description) values (3, 'ATLAS cohort');

DROP TABLE IF EXISTS @schema.data_source;
CREATE TABLE @schema.data_source (
    source_id INT PRIMARY KEY,
    source_name varchar,
    source_key varchar,
    cdm_version varchar,
    db_id varchar,
    version_date date
);

INSERT INTO @schema.data_source (source_id, source_name, source_key) values (-99,'Meta Analysis','Meta Analysis');

DROP TABLE IF EXISTS @schema.reward_version;
create table @schema.reward_version(
   id bool PRIMARY KEY DEFAULT TRUE,
   version text,
   CONSTRAINT uni CHECK (id)
);

INSERT INTO @schema.reward_version (version) values ('0.0.3');

-- Valid negative controls target,outcome pairs
DROP TABLE IF EXISTS @schema.negative_control;
CREATE TABLE @schema.negative_control (
    target_cohort_id BIGINT NOT NULL,
    outcome_cohort_id BIGINT NOT NULL,
    PRIMARY KEY (target_cohort_id, outcome_cohort_id)
);

-- Indication mapping
DROP TABLE IF EXISTS @schema.positive_indication;
CREATE TABLE @schema.positive_indication (
    target_cohort_id BIGINT NOT NULL,
    outcome_cohort_id BIGINT NOT NULL,
    PRIMARY KEY (target_cohort_id, outcome_cohort_id)
);
