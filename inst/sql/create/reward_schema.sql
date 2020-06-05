-- REWARD-B dataset. Each app has its own dataset.

CREATE TABLE IF NOT EXISTS result (
    outcome_cohort_id INT NOT NULL,
    target_cohort_id INT NOT NULL,
    source_id INT NOT NULL,
    calibrated BOOLEAN NOT NULL, -- is the result calibrated with empirical calibration?
    rr NUMERIC,
    log_se_rr NUMERIC,
    c_pt NUMERIC,
    t_pt NUMERIC,
    t_at_risk NUMERIC,
    c_at_risk NUMERIC,
    t_cases NUMERIC,
    lb_95 NUMERIC,
    ub_95 NUMERIC,
    p_value NUMERIC,
    study_design VARCHAR, -- SCC or SCCS
);

CREATE TABLE target (
    target_cohort_id INT NOT NULL,
    drug_concept_id INT NOT NULL,
    name INT,
);

CREATE TABLE outcome_concept (
   outcome_cohort_id INT,
   condition_concept_id INT,
);

CREATE TABLE outcome (
    outcome_cohort_id INT,
    type_id INT,
    name string
);

CREATE TABLE cohort_type (
    cohort_type_id INT,
    description varchar -- e.g. Inpatient diagnosis, two diagnosis codes, ATLAS,
);

CREATE TABLE data_source (
    source_id INT PRIMARY KEY,
    source_name varchar,
    source_key varchar,
);

INSERT INTO data_source (source_id, source_name, source_key) values (10,'Optum SES','CDM_Optum_Extended_SES_v1156');
INSERT INTO data_source (source_id, source_name, source_key) values (11,'IBM CCAE','CDM_IBM_CCAE_V1151');
INSERT INTO data_source (source_id, source_name, source_key) values (12,'IBM MDCD','CDM_IBM_MDCD_v1153');
INSERT INTO data_source (source_id, source_name, source_key) values (13,'IBM MDCR','CDM_IBM_MDCR_v1152');

create table reward_version(
    version NUMERIC
);