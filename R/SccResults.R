getAllExposureIds <- function(connection, config) {
  sql <- "SELECT cohort_definition_id FROM @reference_schema.@cohort_definition"
  queryRes <- DatabaseConnector::renderTranslateQuerySql(
    connection,
    sql,
    reference_schema = config$referenceSchema,
    cohort_definition = config$table$cohortDefinition
  )
  return(queryRes$COHORT_DEFINITION_ID)
}

getAllOutcomeIds <- function(connection, config) {
  sql <- "SELECT cohort_definition_id FROM @reference_schema.@outcome_cohort_definition"
  queryRes <- DatabaseConnector::renderTranslateQuerySql(
    connection,
    sql,
    reference_schema = config$referenceSchema,
    outcome_cohort_definition = config$table$outcomeCohortDefinition
  )
  return(queryRes$COHORT_DEFINITION_ID)
}

getResultsDatabaseTableName <- function(config, dataSource) {
  return(paste0(config$resultsTablePrefix, dataSource$database))
}

SCC_RESULT_COL_NAMES <- c(
  "source_id" = "source_id",
  "analysis_id" = "analysis_id",
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
  analysisId,
  analysisSettings,
  exposureIds = NULL,
  outcomeIds = NULL,
  removeNullValues = FALSE,
  cores = parallel::detectCores() - 1
) {
  ParallelLogger::logInfo(paste("Starting SCC analysis on", config$database))

  if (is.null(exposureIds)) {
    exposureIds <- getAllExposureIds(connection, config)
  }
  if (is.null(outcomeIds)) {
    outcomeIds <- getAllOutcomeIds(connection, config)
  }

  opts <- list(
    connectionDetails = config$connectionDetails,
    cdmDatabaseSchema = config$cdmSchema,
    cdmVersion = 5,
    exposureIds = exposureIds,
    outcomeIds = outcomeIds,
    exposureDatabaseSchema = config$resultSchema,
    exposureTable = config$tables$cohort,
    outcomeDatabaseSchema = config$resultSchema,
    outcomeTable = config$tables$outcomeCohort,
    computeThreads = cores
  )
  args <- c(analysisSettings, opts)

  sccResult <- do.call(SelfControlledCohort::runSelfControlledCohort, args)

  ParallelLogger::logInfo(paste("Completed SCC for", config$database))
  sccSummary <- base::summary(sccResult)

  if (nrow(sccSummary) == 0) {
    ParallelLogger::logError("No results found with scc settings. Check cohorts were run")
    stop("No results found with scc settings. Check cohorts were run")
  }

  sccSummary$p <- EmpiricalCalibration::computeTraditionalP(sccSummary$logRr, sccSummary$seLogRr)
  sccSummary <- base::do.call(data.frame, lapply(sccSummary, function(x) replace(x, is.infinite(x) | is.nan(x), NA)))

  if (removeNullValues) {
    sscSummary <- sccSummary[sccSummary$numOutcomesExposed > 0,]
  }

  sccSummary$source_id <- config$sourceId
  sccSummary$analysis_id <- as.integer(analysisId)
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