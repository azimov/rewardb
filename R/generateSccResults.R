
#' Peform SCC from self controlled cohort package with rewardbs settings
runScc <- function(config, dataSource, exposureIds, outcomeIds, cores = parallel::detectCores() - 1) {
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
  sccSummary <- base::summary(sccResult)
  sccSummary$p <- EmpiricalCalibration::computeTraditionalP(sccSummary$logRr, sccSummary$seLogRr)
  sccSummary <- base::do.call(data.frame, lapply(sccSummary, function(x) replace(x, is.infinite(x) | is.nan(x), NA)))
  return(sccSummary)
}

getResultsDatabaseTableName <- function(config, dataSource) {
  return(paste0(config$resultsTablePrefix, dataSource$database))
}


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

batchScc <- function(connection, config, dataSource, batchSize = 1000) {
  exposureIds <- getAllExposureIds(connection, config)
  outcomeIds <- getAllOutcomeIds(connection, config)
  ParallelLogger::logInfo(paste("Starting SCC batch analysis on", datSource$databse))

  eIndex <- 1
  oIndex <- 1
  while (eIndex < length(exposureIds)) {
    eEnd <- min(eIndex + batchSize - 1, length(exposureIds))
    while (oIndex < length(outcomeIds)) {
      ParallelLogger::logInfo(paste(datSource$databse, "scc batch", eIndex, oIndex, eEnd, oEnd))
      oEnd <- min(oIndex + batchSize - 1, length(outcomeIds))
      sccSummary <- runScc(config, dataSource, exposureIds[eIndex:eEnd], outcomeIds[oIndex:oEnd])
      DatabaseConnector::dbAppendTable(connection, paste0(config$cdmDatabase$schema, ".", getResultsDatabaseTableName(config, dataSource)), sccSummary)
      # Write result to table
      oIndex <- oIndex + batchSize
    }
    eIndex <- eIndex + batchSize
  }

  ParallelLogger::logInfo(paste("Completed SCC batch analysis on", datSource$databse))
}

# Add an individual atlas outcome (or outcomes) to the results set
generateCustomOutcomeResult <- function(connection, config, dataSource, outcomeIds, batchSize = 1000) {
  exposureIds <- getAllExposureIds(connection, config)
  resultsTableName <- paste0(config$cdmDatabase$schema, ".", getResultsDatabaseTableName(config, dataSource))
  eIndex <- 1
  ParallelLogger::logInfo("Starting SCC batch analysis...")
  while (eIndex < length(exposureIds)) {
    eEnd <- min(eIndex + batchSize - 1, length(exposureIds))
    ParallelLogger::logInfo(paste("Stariting batch", eIndex, eEnd))
    sccSummary <- runScc(config, dataSource, exposureIds[eIndex:eEnd], outcomeIds)
    ParallelLogger::logInfo(paste("Appending data to table", resultsTableName))
    DatabaseConnector::dbAppendTable(connection, resultsTableName, sccSummary)
    eIndex <- eIndex + batchSize
  }
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