getAllExposureIds <- function(connection, config) {
  sql <- "SELECT cohort_definition_id FROM @cohort_database_schema.@cohort_definition_table"
  queryRes <- DatabaseConnector::renderTranslateQuerySql(
    connection,
    sql,
    cohort_database_schema = config$cdmDatabase$schema,
    cohort_definition_table = config$cdmDatabase$cohortDefinitionTable
  )
  return(queryRes$COHORT_DEFINITION_ID)
}

getAllOutcomeIds <- function(connection, config) {
  sql <- "SELECT cohort_definition_id FROM @cohort_database_schema.@outcome_cohort_definition_table"
  queryRes <- DatabaseConnector::renderTranslateQuerySql(
    connection,
    sql,
    cohort_database_schema = config$cdmDatabase$schema,
    outcome_cohort_definition_table = config$cdmDatabase$outcomeCohortDefinitionTable
  )
  return(queryRes$COHORT_DEFINITION_ID)
}

getResultsDatabaseTableName <- function(config, dataSource) {
  return(paste0(config$resultsTablePrefix, dataSource$database))
}


#' Peform SCC from self controlled cohort package with rewardbs settings
runScc <- function(
  connection,
  config,
  dataSource,
  exposureIds = NULL,
  outcomeIds = NULL,
  cores = parallel::detectCores() - 1,
  storeResults = TRUE
) {
  ParallelLogger::logInfo(paste("Starting SCC analysis on", dataSource$database))

  if (is.null(exposureIds)) {
    exposureIds <- getAllExposureIds(connection, config)
  }
  if (is.null(outcomeIds)) {
    outcomeIds <- getAllOutcomeIds(connection, config)
  }

  sccResult <- SelfControlledCohort::runSelfControlledCohort(
    connectionDetails = config$cdmDataSource,
    cdmDatabaseSchema = dataSource$cdmDatabaseSchema,
    cdmVersion = 5,
    exposureIds = exposureIds,
    outcomeIds = outcomeIds,
    exposureDatabaseSchema = config$cdmDatabase$schema,
    exposureTable = dataSource$cohortTable,
    outcomeDatabaseSchema = config$cdmDatabase$schema,
    outcomeTable = dataSource$outcomeCohortTable,
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
  ParallelLogger::logInfo(paste("completed SCC analysis on", dataSource$databse))
  sccSummary <- base::summary(sccResult)
  sccSummary$p <- EmpiricalCalibration::computeTraditionalP(sccSummary$logRr, sccSummary$seLogRr)
  sccSummary <- base::do.call(data.frame, lapply(sccSummary, function(x) replace(x, is.infinite(x) | is.nan(x), NA)))

  if (storeResults) {
    tableName <- paste0(config$cdmDatabase$schema, ".", getResultsDatabaseTableName(config, dataSource))
    ParallelLogger::logInfo(paste("Appending result to", tableName))
    DatabaseConnector::dbAppendTable(connection, tableName, sccSummary)
  }
  ParallelLogger::logInfo(paste("Completed SCC analysis on", dataSource$databse))

  sccSummary$source_id <- dataSource$sourceId
  sccSummary$analysis_id <- 1
  sccSummary <- dplyr::rename(sccSummary, c(
      "target_cohort_id" = "exposureId",
      "outcome_cohort_id" = "outcomeId",
      "t_at_risk" = "numPersons",
      "t_pt" = "timeAtRiskExposed",
      "t_cases" = "numOutcomesExposed",
      "c_at_risk" = "numPersons",
      "c_cases" = "numOutcomesUnexposed",
      "c_pt" = "timeAtRiskUnexposed",
      "relative_risk" = "irr",
      "lb_95" = "irrLb95",
      "ub_95" = "irrUb95",
      "log_rr" = "logRr",
       "se_log_rr" = "seLogRr",
      "p_value" = "p"
    ))


  return(sccSummary)
}

getResultsDatabaseTableName <- function(config, dataSource) {
  return(paste0(config$resultsTablePrefix, dataSource$database))
}

#' @export
createResultsTables <- function(connection, config, dataSources) {
  for (ds in dataSources) {
    dataSource <- config$dataSources[[ds]]
    sql <- SqlRender::readSql(system.file("sql/create", "createResultsTable.sql", package = "rewardb"))
    DatabaseConnector::renderTranslateExecuteSql(
      connection,
      sql,
      results_database_schema = config$cdmDatabase$schema,
      results_table = getResultsDatabaseTableName(config, dataSource)
    )
  }
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
  for (ds in dataSources) {
    dataSource <- config$dataSources[[ds]]
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
  for (ds in dataSources) {
    dataSource <- config$dataSources[[ds]]
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
  tableName <- paste0(config$cdmDatabase$schema, config$cdmDatabase$mergedResultsTable)
  DatabaseConnector::dbAppendTable(connection, tableName, results)
}