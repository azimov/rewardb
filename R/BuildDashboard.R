createDashSchema <- function(appContext, connection) {
  DatabaseConnector::executeSql(connection, paste("DROP SCHEMA IF EXISTS", appContext$short_name, "CASCADE;"))
  DatabaseConnector::executeSql(connection, paste("CREATE SCHEMA ", appContext$short_name))

  ParallelLogger::logInfo("Creating tables")
  pathToSqlFile <- system.file("sql/create", "rewardSchema.sql", package = "rewardb")
  sql <- SqlRender::readSql(pathToSqlFile)
  sql <- SqlRender::render(sql, schema = appContext$short_name)
  DatabaseConnector::executeSql(connection, sql = sql)

  ParallelLogger::logInfo("Mapping concepts to cohorts")
  outcomeCohortIds <- getOutcomeCohortIds(appContext, connection)
  targetCohortIds <- getTargetCohortIds(appContext, connection)

  ParallelLogger::logInfo("Importing references")
  pathToSqlFile <- system.file("sql/export", "buildDashboard.sql", package = "rewardb")
  sql <- SqlRender::readSql(pathToSqlFile)
  sql <- DatabaseConnector::renderTranslateExecuteSql(
    connection,
    sql,
    schema = appContext$short_name,
    outcome_cohort_ids_length = length(outcomeCohortIds) > 0,
    target_cohort_ids_length = length(targetCohortIds) > 0,
    target_cohort_ids = targetCohortIds,
    outcome_cohort_ids = outcomeCohortIds,
    vocabulary_schema = appContext$globalConfig$vocabularySchema,
    results_database_schema = appContext$globalConfig$rewardbResultsSchema
  )
}

#'
#' ing a CEM source finds any evidence related to conditions and exposures
#' This is used for the automated construction of negative control sets and the indication labels
#' @param appContext rewardb app context
addCemEvidence <- function(appContext, connection) {
  library(dplyr)
  evidenceConcepts <- getStudyControls(
    connection,
    schema = appContext$globalConfig$rewardbResultsSchema,
    cemSchema = appContext$globalConfig$cemSchema,
    vocabularySchema = appContext$globalConfig$vocabularySchema,
    targetCohortIds = getOutcomeCohortIds(appContext, connection),
    outcomeCohortIds = getTargetCohortIds(appContext, connection)
  )

  ParallelLogger::logInfo(paste("Found", nrow(evidenceConcepts), "mappings"))
  negatives <- evidenceConcepts[evidenceConcepts$EVIDENCE == 0,]
  positives <- evidenceConcepts[evidenceConcepts$EVIDENCE == 0,]
  if (nrow(negatives) == 0 | nrow(positives) == 0) {
    ParallelLogger::logWarn("Zero control/indication references found. Likely due to a cohort mapping problem")
  } else {
    DatabaseConnector::insertTable(appContext$connection, paste0(appContext$short_name, ".negative_control"), negatives)
    DatabaseConnector::insertTable(appContext$connection, paste0(appContext$short_name, ".positive_indication"), positives)
  }
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
computeMetaAnalysis <- function(appContext, connection) {
  library(dplyr, warn.conflicts = FALSE)
  fullResults <- DatabaseConnector::renderTranslateQuerySql(
    connection,
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

  resultsTable <- paste0(appContext$short_name, ".result")
  DatabaseConnector::dbAppendTable(connection, resultsTable, results)
}


#' Builds a rewardb dashboard for all exposures for subset outcomes or all outcomes for subset exposures
#' Requires the rewardb results to be generated already.
#' This exports the data to a db backend and allows the config file to be used to run the shiny dashboard
#' @param filePath - path to a yaml configuration file used
#' @param performCalibration - use empirical calibration package to compute adjusted p values, effect estimates and confidence intervals
#' @export
buildFromConfig <- function(filePath, globalConfigPath, performCalibration = TRUE) {
  appContext <- loadAppContext(filePath, globalConfigPath)
  connection <- DatabaseConnector::connect(appContext$connectionDetails)

  message("Creating schema")
  createDashSchema(appContext, connection)
  message("Adding negative controls from CEM")
  addCemEvidence(appContext, connection)
  message("Running meta analysis")
  computeMetaAnalysis(appContext, connection)

  if (performCalibration) {
    .removeCalibratedResults(appContext, connection)
    if (appContext$useExposureControls) {
      message("Calibrating outcomes")
      calibrateOutcomes(appContext, connection)
    } else {
      message("Calibrating targets")
      calibrateTargets(appContext, connection)
    }
  }
  DatabaseConnector::disconnect(connection)
}