#' Map target cohorts to names and exposure classes
#' @param appContext rewardb app context
extractTargetCohortNames <- function (appContext) {
  library(dplyr)
  sql <- "
  SELECT
    t.cohort_definition_id AS target_cohort_id,
    t.short_name AS cohort_name,
    CASE
      WHEN c1.concept_class_id = 'ATC 4th' THEN 1
      ELSE 0
    END AS is_atc_4,
    0 AS is_custom_cohort
  FROM @results_database_schema.@cohort_definition_table t
  INNER JOIN @cdm_vocabulary.concept c1 ON t.cohort_definition_id/1000 = c1.concept_id
  {@fixed_target_cohorts} ? {   WHERE t.cohort_definition_id IN (@target_cohort_ids)}
  {@use_custom_exposure_ids} ? {
    UNION

    SELECT
      custom_exposure_id AS target_cohort_id,
      cohort_name,
      0 AS is_atc_4,
      1 AS is_custom_cohort
    FROM @results_database_schema.custom_exposure
    {@fixed_custom_exposures} ? {   WHERE custom_exposure_id IN (@custom_exposure_ids)}
  }
  ";
  useCustomExposureIds = (length(appContext$target_concept_ids) > 0 & length(appContext$custom_exposure_ids) > 0 ) | length(appContext$target_concept_ids) == 0
  nameSet <- DatabaseConnector::renderTranslateQuerySql(
    appContext$cdmConnection,
    sql,
    cdm_vocabulary = appContext$resultsDatabase$vocabularySchema,
    fixed_target_cohorts = length(appContext$target_concept_ids) > 0,
    use_custom_exposure_ids = useCustomExposureIds,
    fixed_custom_exposures = length(appContext$custom_exposure_ids) > 0,
    target_cohort_ids = appContext$target_concept_ids * 1000,
    custom_exposure_ids = appContext$custom_exposure_ids,
    cohort_definition_table = appContext$resultsDatabase$cohortDefinitionTable,
    results_database_schema = appContext$resultsDatabase$schema
  )

  DatabaseConnector::dbAppendTable(appContext$connection, paste0(appContext$short_name, ".target"), nameSet)

  # Add target concept mappings
  sql <- "
  SELECT
    t.cohort_definition_id AS target_cohort_id,
    t.cohort_definition_id/1000 AS concept_id,
    0 AS is_excluded,
    0 AS include_descendants
  FROM @results_database_schema.@cohort_definition_table t
  {@fixed_target_cohorts} ? {   WHERE t.cohort_definition_id IN (@target_cohort_ids)}

  {@use_custom_exposure_ids} ? {
  UNION
  SELECT
    cec.custom_exposure_id AS target_cohort_id,
    cec.concept_id AS concept_id,
    cec.is_excluded AS is_excluded,
    cec.include_descendants AS include_descendants
  FROM @results_database_schema.custom_exposure_concept cec
  {@fixed_custom_exposures} ? {   WHERE cec.custom_exposure_id IN (@custom_exposure_ids)} }
  "

  conceptMapping <- DatabaseConnector::renderTranslateQuerySql(
    appContext$cdmConnection,
    sql,
    fixed_target_cohorts = length(appContext$target_concept_ids) > 0,
    use_custom_exposure_ids = useCustomExposureIds,
    fixed_custom_exposures = length(appContext$custom_exposure_ids) > 0,
    target_cohort_ids = appContext$target_concept_ids * 1000,
    custom_exposure_ids = appContext$custom_exposure_ids,
    cohort_definition_table = appContext$resultsDatabase$cohortDefinitionTable,
    results_database_schema = appContext$resultsDatabase$schema
  )

 DatabaseConnector::dbAppendTable(appContext$connection, paste0(appContext$short_name, ".target_concept"), conceptMapping)

  # Add exposure classes
  sql <- "SELECT t.cohort_definition_id as target_cohort_id, c.concept_id as exposure_class_id, c.concept_name as exposure_class_name
    FROM @cdm_vocabulary.concept_ancestor ca
    INNER JOIN @cdm_vocabulary.concept c on (ca.ancestor_concept_id = c.concept_id AND c.concept_class_id = 'ATC 3rd')
    INNER JOIN @results_database_schema.@cohort_definition_table t ON (t.cohort_definition_id/1000 = ca.descendant_concept_id)
   {@fixed_target_cohorts} ? {WHERE t.cohort_definition_id IN (@target_cohort_ids)}"


  nameSet <- DatabaseConnector::renderTranslateQuerySql(
    appContext$cdmConnection, sql,
    cdm_vocabulary = appContext$resultsDatabase$vocabularySchema,
    fixed_target_cohorts = length(appContext$target_concept_ids) > 0,
    target_cohort_ids = appContext$target_concept_ids * 1000,
    cohort_definition_table = appContext$resultsDatabase$cohortDefinitionTable,
    results_database_schema = appContext$resultsDatabase$schema
  )

  exposureClasses <- distinct(nameSet, EXPOSURE_CLASS_ID, EXPOSURE_CLASS_NAME)
  DatabaseConnector::dbAppendTable(appContext$connection, paste0(appContext$short_name, ".exposure_class"), exposureClasses)
  exposureClasses <- distinct(nameSet, TARGET_COHORT_ID, EXPOSURE_CLASS_ID)
  DatabaseConnector::dbAppendTable(appContext$connection, paste0(appContext$short_name, ".target_exposure_class"), exposureClasses)
}

#' Map outcome cohorts to names
#' @param appContext rewardb app context
extractOutcomeCohortNames <- function (appContext) {
  sql <- " 
      SELECT 
        o.cohort_definition_id AS outcome_cohort_id,
        o.short_name AS cohort_name, 
        o.cohort_definition_id % 100 as type_id
      from @results_database_schema.@outcome_cohort_definition_table o
      LEFT JOIN @results_database_schema.@atlas_cohort_definition_table acd ON acd.cohort_definition_id = o.cohort_definition_id
      WHERE acd.cohort_definition_id IS NULL
      {@outcome_cohort_ids_length} ? {AND o.cohort_definition_id IN (@outcome_cohort_ids)}
      UNION
      -- Only pull specified subset of atlas cohorts
      SELECT
          acd.cohort_definition_id AS outcome_cohort_id,
          acd.cohort_name AS cohort_name,
          2 as type_id
        FROM @results_database_schema.@atlas_cohort_definition_table acd
      {@outcome_cohort_ids_length} ? {WHERE acd.cohort_definition_id IN (@custom_outcome_ids)}
  ";
  outcomeIds <- append(appContext$outcome_concept_ids * 100, appContext$outcome_concept_ids * 100 + 1)
  nameSet <- DatabaseConnector::renderTranslateQuerySql(
      appContext$cdmConnection,
      sql,
      outcome_cohort_ids = if(length(outcomeIds)) outcomeIds else "NULL",
      atlas_cohort_definition_table=appContext$resultsDatabase$atlasCohorts,
      custom_outcome_ids = if(length(appContext$custom_outcome_cohort_ids)) appContext$custom_outcome_cohort_ids else "NULL",
      outcome_cohort_ids_length = length(outcomeIds) > 0 | length(appContext$custom_outcome_cohort_ids),
      outcome_cohort_definition_table=appContext$resultsDatabase$outcomeCohortDefinitionTable,
      results_database_schema=appContext$resultsDatabase$schema
    )
  DatabaseConnector::dbAppendTable(appContext$connection, paste0(appContext$short_name, ".outcome"), nameSet)
}

#' Map outcomes to concept ids
#' @param appContext rewardb app context
createOutcomeConceptMapping <- function (appContext) {
  sql <- "INSERT INTO @schema.outcome_concept (outcome_cohort_id, condition_concept_id)
    SELECT outcome_cohort_id, outcome_cohort_id/100 AS condition_concept_id
        FROM @schema.outcome WHERE type_id IN (0, 1);"

  DatabaseConnector::renderTranslateExecuteSql(appContext$connection, sql, schema=appContext$short_name)

  sql <- "
  SELECT acd.cohort_definition_id as outcome_cohort_id, acd.concept_id as condition_concept_id
  FROM @results_database_schema.@atlas_cohort_definition_table acd
  WHERE acd.is_excluded = 0
  {@subset_atlas_cohorts} ? {AND acd.cohort_definition_id IN (@atlas_cohort_ids)}

  UNION

  SELECT acd.cohort_definition_id as outcome_cohort_id, ca.descendant_concept_id as condition_concept_id
  FROM @results_database_schema.@atlas_cohort_definition_table acd
  INNER JOIN @cdm_vocabulary.concept_ancestor ca ON ca.ancestor_concept_id = acd.concept_id
  WHERE acd.include_descendants = 1
  AND acd.is_excluded = 0
  {@subset_atlas_cohorts} ? {AND acd.cohort_definition_id IN (@atlas_cohort_ids)}
  "

  data <- DatabaseConnector::renderTranslateQuerySql(
    appContext$cdmConnection,
    sql,
    cdm_vocabulary = appContext$resultsDatabase$vocabularySchema,
    results_database_schema=appContext$resultsDatabase$schema,
    atlas_cohort_definition_table=appContext$resultsDatabase$atlasConceptMapping,
    atlas_cohort_ids=appContext$custom_outcome_cohort_ids,
    subset_atlas_cohorts  = length(appContext$custom_outcome_cohort_ids) > 0
  )
  DatabaseConnector::dbAppendTable(appContext$connection, paste(appContext$short_name, ".outcome_concept"), unique(data))
}

#' Takes results from a CDM and puts them in the rewardb schema
#' @param appContext rewardb app context
extractResultsSubset <- function(appContext){

  sql <- "
    select
    DISTINCT
      scca.source_id,
      scca.target_cohort_id,
      scca.outcome_cohort_id,
      scca.t_at_risk,
      scca.t_pt,
      scca.t_cases,
      scca.c_at_risk,
      scca.c_pt,
      scca.c_cases,
      EXP(scca.log_rr) rr,
      scca.lb_95,
      scca.ub_95,
      scca.se_log_rr,
      scca.p_value
      from @results_database_schema.@scca_results scca
      WHERE scca.log_rr IS NOT NULL
      {@target_cohort_ids_length} ? {AND target_cohort_id in (@target_cohort_ids)}
      {@outcome_cohort_ids_length} ? {AND outcome_cohort_id in (@outcome_cohort_ids)}
  "

  targetCohorts <- append(appContext$target_concept_ids * 1000, appContext$custom_exposure_ids)
  outcomeCohortIds <- append(appContext$outcome_concept_ids * 100, appContext$outcome_concept_ids * 100 + 1)
  resultSet <- DatabaseConnector::renderTranslateQuerySql(appContext$cdmConnection, sql,
                                                           target_cohort_ids = targetCohorts,
                                                           target_cohort_ids_length = length(targetCohorts)  > 0,
                                                           outcome_cohort_ids = append(outcomeCohortIds, appContext$custom_outcome_cohort_ids),
                                                           outcome_cohort_ids_length = length(outcomeCohortIds)  > 0,
                                                           scca_results = appContext$resultsDatabase$asurvResultsTable,
                                                           results_database_schema = appContext$resultsDatabase$schema)

  resultSet$study_design <- "scc"
  DatabaseConnector::dbAppendTable(appContext$connection,paste0(appContext$short_name, ".result"), resultSet)
}

createTables <- function (appContext) {
  pathToSqlFile <- system.file("sql/create", "reward_schema.sql", package = "rewardb")
  sql <- SqlRender::readSql(pathToSqlFile)
  sql <- SqlRender::render(sql, schema=appContext$short_name)
  DatabaseConnector::executeSql(appContext$connection, sql = sql)
}

#'
#' ing a CEM source finds any evidence related to conditions and exposures
#' This is used for the automated construction of negative control sets and the indication labels
#' TODO: test and improve this logic and move to its own R file
#' @param appContext rewardb app context
addCemEvidence <- function(appContext) {
  library(dplyr)
  evidenceConcepts <- getStudyControls(
    appContext$cdmConnection,
    appContext$resultsDatabase$schema,
    appContext$resultsDatabase$cemSchema,
    appContext$resultsDatabase$cohortDefinitionTable,
    appContext$resultsDatabase$outcomeCohortDefinitionTable,
    appContext$resultsDatabase$atlasConceptMapping,
    targetCohortIds = appContext$targetCohortIds,
    outcomeCohortIds = appContext$outcomeCohortIds,
    atlasOutcomeIds = appContext$custom_outcome_cohort_ids,
    atlasTargetIds = appContext$custom_exposure_ids
  )

  ParallelLogger::logInfo(paste("Found", nrow(evidenceConcepts), "mappings"))

  DatabaseConnector::insertTable(appContext$connection, paste0(appContext$short_name, ".negative_control"), evidenceConcepts[evidenceConcepts$EVIDENCE == 0,])
  DatabaseConnector::insertTable(appContext$connection, paste0(appContext$short_name, ".positive_indication"), evidenceConcepts[evidenceConcepts$EVIDENCE == 1,])
}

#' Perform meta-analysis on data sources
#' @param table data.frame
#' @return data.frame - results of meta analysis
metaAnalysis <- function(table) {
  # Compute meta analysis with random effects model
  results <- meta::metainc(
    data = table,
    event.e = T_CASES,
    time.e = T_PT,
    event.c = C_CASES,
    time.c = C_PT,
    sm = "IRR",
    model.glmm = "UM.RS"
  )

  row <- data.frame(
    SOURCE_ID = -99,
    T_AT_RISK = sum(table$T_AT_RISK),
    T_PT = sum(table$T_PT),
    T_CASES = sum(table$T_CASES),
    C_AT_RISK = sum(table$C_AT_RISK),
    C_PT = sum(table$C_PT),
    C_CASES = sum(table$C_CASES),
    RR = exp(results$TE.random),
    SE_LOG_RR = results$seTE.random,
    LB_95 = exp(results$lower.random),
    UB_95 = exp(results$upper.random),
    P_VALUE = results$pval.random,
    I2 = results$I2
  )

  return(row)
}

#' Runs and saves metanalayis on data
#' @param appContext
performMetaAnalysis <- function(appContext) {
  library(dplyr, warn.conflicts = FALSE)
  fullResults <- DatabaseConnector::renderTranslateQuerySql(
    appContext$connection,
    "SELECT * FROM @schema.result WHERE source_id != -99;",
    schema=appContext$short_name
  )

  # For each distinct pair: (target, outcome) get all data sources
  # Run meta analysis
  # Write uncalibrated table
  # Calibrate meta analysis results
  results <- fullResults %>%
    group_by(TARGET_COHORT_ID, OUTCOME_COHORT_ID) %>%
    group_modify(~ metaAnalysis(.x))

  results$STUDY_DESIGN <- "scc"
  results$CALIBRATED <- 0

  resultsTable <- paste(appContext$short_name, ".result")
  DatabaseConnector::dbAppendTable(appContext$connection, resultsTable, data.frame(results))
}

#' Builds a rewardb dashboard for all exposures for subset outcomes or all outcomes for subset exposures
#' Requires the rewardb results to be generated already.
#' This exports the data to a db backend and allows the config file to be used to run the shiny dashboard
#' @param filePath - path to a yaml configuration file used
#' @param calibrateOutcomes - where subset of outcomes is specified
#' @param calibrateTargets - where a subset of target exposures is specified
#' @export
buildFromConfig <- function(filePath, calibrateOutcomes = FALSE, calibrateTargets = FALSE) {
  appContext <- loadAppContext(filePath, createConnection = TRUE, useCdm = TRUE)
  message("Creating schema")
  DatabaseConnector::executeSql(appContext$connection, paste("DROP SCHEMA IF EXISTS", appContext$short_name, "CASCADE;"))
  DatabaseConnector::executeSql(appContext$connection, paste("CREATE SCHEMA ", appContext$short_name))
  createTables(appContext)

  message("Extracting results from CDM data source")
  extractResultsSubset(appContext)
  message("Extracting cohort names")
  extractTargetCohortNames(appContext)
  message("Extracting outcome cohort names")
  extractOutcomeCohortNames(appContext)
  message("Extracting outcome cohort mapping")
  createOutcomeConceptMapping(appContext)
  message("Adding negative controls from CEM")
  addCemEvidence(appContext)
  message("Running meta analysis")
  performMetaAnalysis(appContext)
  DatabaseConnector::disconnect(appContext$connection)
  DatabaseConnector::disconnect(appContext$cdmConnection)

  if (calibrateTargets) {
    message("Calibrating targets")
    .removeCalibratedResults(appContext)
    rewardb::calibrateTargets(appContext)
  }

  if (calibrateOutcomes) {
   message("Calibrating outcomes")
   rewardb::calibrateOutcomes(appContext)
   rewardb::calibrateOutcomesCustomCohorts(appContext)
  }
}