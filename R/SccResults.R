getAllExposureIds <- function(connection, config) {
  sql <- "SELECT cohort_definition_id FROM @reference_schema.cohort_definition"
  queryRes <- DatabaseConnector::renderTranslateQuerySql(
    connection,
    sql,
    reference_schema = config$referenceSchema
  )
  return(queryRes$COHORT_DEFINITION_ID)
}

getAllOutcomeIds <- function(connection, config) {
  sql <- "SELECT cohort_definition_id FROM @reference_schema.outcome_cohort_definition"
  queryRes <- DatabaseConnector::renderTranslateQuerySql(
    connection,
    sql,
    reference_schema = config$referenceSchema
  )
  return(queryRes$COHORT_DEFINITION_ID)
}

getResultsDatabaseTableName <- function(config, dataSource) {
  return(paste0(config$resultsTablePrefix, dataSource$database))
}

SCC_RESULT_COL_NAMES <- c(
  "source_id" = "source_id",
  "target_cohort_id" = "exposureId",
  "outcome_cohort_id" = "outcomeId",
  "t_at_risk" = "numPersons",
  "t_pt" = "timeAtRiskExposed",
  "t_cases" = "numOutcomesExposed",
  "c_cases" = "numOutcomesUnexposed",
  "c_pt" = "timeAtRiskUnexposed",
  "rr" = "irr",
  "lb_95" = "irrLb95",
  "ub_95" = "irrUb95",
  "se_log_rr" = "seLogRr",
  "p_value" = "p"
)

#' Peform SCC from self controlled cohort package with rewardbs settings
runScc <- function(
  connection,
  config,
  exposureIds = NULL,
  outcomeIds = NULL,
  cores = parallel::detectCores() - 1
) {
  ParallelLogger::logInfo(paste("Starting SCC analysis on", config$database))

  if (is.null(exposureIds)) {
    exposureIds <- getAllExposureIds(connection, config)
  }
  if (is.null(outcomeIds)) {
    outcomeIds <- getAllOutcomeIds(connection, config)
  }

  sccResult <- SelfControlledCohort::runSelfControlledCohort(
    connectionDetails = config$connectionDetails,
    cdmDatabaseSchema = config$cdmSchema,
    cdmVersion = 5,
    exposureIds = exposureIds,
    outcomeIds = outcomeIds,
    exposureDatabaseSchema = config$resultSchema,
    exposureTable = config$tables$cohort,
    outcomeDatabaseSchema = config$resultSchema,
    outcomeTable = config$tables$outcomeCohort,
    # Settings made in original activesurvelance_dev package
    firstExposureOnly = TRUE,
    firstOutcomeOnly = TRUE,
    minAge = "",
    maxAge = "",
    studyStartDate = "",
    studyEndDate = "",
    addLengthOfExposureExposed = TRUE,
    riskWindowStartExposed = 1,
    riskWindowEndExposed = 1,
    addLengthOfExposureUnexposed = TRUE,
    riskWindowEndUnexposed = -1,
    riskWindowStartUnexposed = -1,
    hasFullTimeAtRisk = TRUE,
    washoutPeriod = 0,
    followupPeriod = 0,
    computeThreads = cores
  )
  ParallelLogger::logInfo(paste("Completed SCC for", config$database))
  sccSummary <- base::summary(sccResult)
  sccSummary$p <- EmpiricalCalibration::computeTraditionalP(sccSummary$logRr, sccSummary$seLogRr)
  sccSummary <- base::do.call(data.frame, lapply(sccSummary, function(x) replace(x, is.infinite(x) | is.nan(x), NA)))
  sscSummary <- sccSummary[sccSummary$numOutcomesExposed > 0,]

  sccSummary$source_id <- config$sourceId
  sccSummary$analysis_id <- 1
  sccSummary$c_at_risk <- sccSummary$numPersons

  sccSummary <- dplyr::rename(sccSummary, rewardb::SCC_RESULT_COL_NAMES)

  ParallelLogger::logInfo(paste("Generated results for SCC", config$database))

  return(sccSummary)
}

getResultsDatabaseTableName <- function(config, dataSource) {
  return(paste0(config$resultsTablePrefix, dataSource$database))
}

createResultsTable <- function(connection, config, dataSource) {
  sql <- SqlRender::readSql(system.file("sql/create", "createResultsTable.sql", package = "rewardb"))
  DatabaseConnector::renderTranslateExecuteSql(
    connection,
    sql,
    results_database_schema = config$cdmDatabase$schema,
    results_table = getResultsDatabaseTableName(config, dataSource)
  )
}

# Place results from all data sources in a single table
#' @export

compileResults <- function(connection, config) {

  sql <- SqlRender::readSql(system.file("sql/create", "createMergedResultsTable.sql", package = "rewardb"))
  # create asurv table
  DatabaseConnector::renderTranslateExecuteSql(
    connection,
    sql,
    results_database_schema = config$cdmDatabase$schema,
    merged_results_table = config$cdmDatabase$mergedResultsTable
  )

  # compile results from different data sources
  for (dataSource in dataSources) {
    sql <- SqlRender::readSql(system.file("sql/create", "compileResults.sql", package = "rewardb"))
    DatabaseConnector::renderTranslateExecuteSql(
      connection,
      sql,
      source_id = dataSource$sourceId,
      results_database_schema = config$cdmDatabase$schema,
      merged_results_table = config$cdmDatabase$mergedResultsTable,
      results_table = getResultsDatabaseTableName(config, dataSource),
      use_custom_ids = 0
    )
  }
}

addAtlasResultsToMergedTable <- function(connection, config, atlasIds, dataSources) {
  for (dataSource in dataSources) {
    sql <- SqlRender::readSql(system.file("sql/create", "compileResults.sql", package = "rewardb"))
    DatabaseConnector::renderTranslateExecuteSql(
      connection,
      sql,
      source_id = dataSource$sourceId,
      results_database_schema = config$cdmDatabase$schema,
      merged_results_table = config$cdmDatabase$mergedResultsTable,
      results_table = getResultsDatabaseTableName(config, dataSource),
      use_custom_ids = 1,
      custom_outcome_ids = atlasIds
    )
  }
}

addCsvAtlasResultsToMergedTable <- function(connection,
                                            config,
                                            results,
                                            removeOutcomeIds = FALSE,
                                            removeCustomExposureIds = FALSE
) {
  # Stops duplicate entries but use with care
  if (removeOutcomeIds) {
    DatabaseConnector::renderTranslateExecuteSql(
      connection = connection,
      sql = "DELETE FROM @results_database_schema.@merged_results_table WHERE outcome_cohort_id IN (@outcome_cohort_ids)",
      results_database_schema = config$cdmDatabase$schema,
      merged_results_table = config$cdmDatabase$mergedResultsTable,
      outcome_cohort_ids = unique(results$outcome_cohort_id)
    )
  }

  if (removeCustomExposureIds) {
    DatabaseConnector::renderTranslateExecuteSql(
      connection = connection,
      sql = "DELETE FROM @results_database_schema.@merged_results_table WHERE exposure_cohort_id IN (@exposure_cohort_ids)",
      results_database_schema = config$cdmDatabase$schema,
      merged_results_table = config$cdmDatabase$mergedResultsTable,
      exposure_cohort_ids = unique(results$exposure_cohort_id)
    )
  }
  tableName <- paste0(config$cdmDatabase$schema, ".", config$cdmDatabase$mergedResultsTable)

  colnames <- c("analysis_id", "source_id", "target_cohort_id", "outcome_cohort_id", "t_at_risk", "t_pt", "t_cases",
                "c_at_risk", "c_cases", "c_pt", "relative_risk", "lb_95", "ub_95", "log_rr", "se_log_rr", "p_value")
  DatabaseConnector::dbAppendTable(connection, tableName, results[colnames])
}