-- Results
INSERT INTO @schema.result
(
    analysis_id,
    source_id,
    target_cohort_id,
    outcome_cohort_id,
    t_at_risk,
    t_pt,
    t_cases,
    c_at_risk,
    c_pt,
    c_cases,
    rr,
    lb_95,
    ub_95,
    se_log_rr,
    p_value,
    mean_time_to_outcome,
    sd_time_to_outcome,
    min_time_to_outcome,
    p10_time_to_outcome,
    p25_time_to_outcome,
    median_time_to_outcome,
    p75_time_to_outcome,
    p90_time_to_outcome,
    max_time_to_outcome,
    mean_tx_time,
    sd_tx_time,
    min_tx_time,
    p10_tx_time,
    p25_tx_time,
    median_tx_time,
    p75_tx_time,
    p90_tx_time,
    max_tx_time,
    study_design,
    calibrated
)
SELECT
  scca.analysis_id,
  scca.source_id,
  scca.target_cohort_id,
  scca.outcome_cohort_id,
  scca.t_at_risk,
  scca.t_pt,
  scca.t_cases,
  scca.c_at_risk,
  scca.c_pt,
  scca.c_cases,
  scca.rr,
  scca.lb_95,
  scca.ub_95,
  scca.se_log_rr,
  scca.p_value,
  scca.mean_time_to_outcome,
  scca.sd_time_to_outcome,
  scca.min_time_to_outcome,
  scca.p10_time_to_outcome,
  scca.p25_time_to_outcome,
  scca.median_time_to_outcome,
  scca.p75_time_to_outcome,
  scca.p90_time_to_outcome,
  scca.max_time_to_outcome,
  scca.mean_tx_time,
  scca.sd_tx_time,
  scca.min_tx_time,
  scca.p10_tx_time,
  scca.p25_tx_time,
  scca.median_tx_time,
  scca.p75_tx_time,
  scca.p90_tx_time,
  scca.max_tx_time,
  'scca' as study_design,
  0 as calibrated
  FROM @results_database_schema.scc_result scca
  WHERE scca.rr IS NOT NULL
  AND scca.analysis_id = 1
  {@source_ids != ''} ? {AND scca.source_id in (@source_ids)}
  {@target_cohort_ids_length} ? {AND target_cohort_id in (@target_cohort_ids)}
  {@outcome_cohort_ids_length} ? {AND outcome_cohort_id in (@outcome_cohort_ids)};

INSERT INTO @schema.target (
    target_cohort_id,
    cohort_name,
    is_atc_4
)
SELECT
    cohort_definition_id as target_cohort_id,
    cohort_definition_name as cohort_name,
    atc_flg as is_atc_4
    from @results_database_schema.cohort_definition
    {@target_cohort_ids_length} ? {WHERE cohort_definition_id in (@target_cohort_ids)}
;

INSERT INTO @schema.target_concept (
    target_cohort_id,
    concept_id,
    is_excluded,
    include_descendants
)
 SELECT
    cd.cohort_definition_id as target_cohort_id,
    csd.concept_id,
    csd.is_excluded,
    csd.include_descendants
 from @results_database_schema.cohort_definition cd
 INNER JOIN @results_database_schema.concept_set_definition csd ON cd.drug_conceptset_id = csd.conceptset_id
 {@target_cohort_ids_length} ? {WHERE cd.cohort_definition_id in (@target_cohort_ids)}
;

INSERT INTO @schema.target_concept (
    target_cohort_id,
    concept_id,
    is_excluded,
    include_descendants
)
 SELECT
    cohort_definition_id as target_cohort_id,
    concept_id,
    is_excluded,
    include_descendants
 from @results_database_schema.custom_exposure_concept
 {@target_cohort_ids_length} ? {WHERE cohort_definition_id in (@target_cohort_ids)}
;

INSERT INTO @schema.target_concept (
    target_cohort_id,
    concept_id,
    is_excluded,
    include_descendants
)
SELECT
    DISTINCT
    cohort_definition_id as target_cohort_id,
    concept_id,
    is_excluded,
    include_descendants
FROM @results_database_schema.atlas_exposure_concept
{@target_cohort_ids_length} ? {WHERE cohort_definition_id in (@target_cohort_ids)};

INSERT INTO @schema.outcome (
    outcome_cohort_id,
    type_id,
    cohort_name
)
SELECT
    cohort_definition_id as outcome_cohort_id,
    outcome_type as type_id,
    short_name as cohort_name
FROM @results_database_schema.outcome_cohort_definition
    {@outcome_cohort_ids_length} ? {WHERE cohort_definition_id in (@outcome_cohort_ids)}
;

INSERT INTO @schema.outcome_concept (
   outcome_cohort_id,
   condition_concept_id
)
SELECT
    DISTINCT
    cohort_definition_id as outcome_cohort_id,
    conceptset_id as condition_concept_id
FROM @results_database_schema.outcome_cohort_definition
WHERE conceptset_id != 99999999
{@outcome_cohort_ids_length} ? {AND cohort_definition_id in (@outcome_cohort_ids)};


INSERT INTO @schema.outcome_concept (
   outcome_cohort_id,
   condition_concept_id
)
SELECT
    DISTINCT
    cohort_definition_id as outcome_cohort_id,
    concept_id as condition_concept_id
FROM @results_database_schema.atlas_outcome_concept
{@outcome_cohort_ids_length} ? {WHERE cohort_definition_id in (@outcome_cohort_ids)};

INSERT INTO @schema.target_exposure_class (
    target_cohort_id,
    exposure_class_id
)
SELECT DISTINCT
    t.cohort_definition_id as target_cohort_id,
    c.concept_id as exposure_class_id
FROM @vocabulary_schema.concept_ancestor ca
    INNER JOIN @vocabulary_schema.concept c on (ca.ancestor_concept_id = c.concept_id AND c.concept_class_id = 'ATC 3rd')
    INNER JOIN @results_database_schema.cohort_definition t ON (t.drug_CONCEPTSET_ID = ca.descendant_concept_id)
    {@target_cohort_ids_length} ? {WHERE t.cohort_definition_id in (@target_cohort_ids)}
;

INSERT INTO  @schema.exposure_class (
    exposure_class_id,
    exposure_class_name
)
SELECT
    tec.exposure_class_id,
    c.concept_name as exposure_class_name
FROM @schema.target_exposure_class tec
INNER JOIN @vocabulary_schema.concept c ON tec.exposure_class_id = c.concept_id
;

INSERT INTO @schema.data_source (
    source_id,
    source_name,
    source_key,
    cdm_version,
    db_id,
    version_date
)
SELECT * FROM @results_database_schema.data_source
{@source_ids != ''} ? {WHERE source_id in (@source_ids)}
;