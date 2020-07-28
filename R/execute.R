#'@export
fullExecution <- function(
  configFilePath = "config/global-cfg.yml",
  createReferences = TRUE,
  addDefaultAtlasCohorts = TRUE,
  .createExposureCohorts = TRUE,
  .createOutcomeCohorts = TRUE,
  .generateSummaryTables = TRUE,
  .runSCC = TRUE,
  logFileName = "rbDataBuild.log"
) {
  logger <- ParallelLogger::createLogger(
    name="DataGen",
    threshold="INFO",
    appenders = list(
      ParallelLogger::createFileAppender(layout = ParallelLogger::layoutTimestamp, fileName = logFileName ),
      ParallelLogger::createConsoleAppender(layout = ParallelLogger::layoutTimestamp)
    )
  )
  ParallelLogger::registerLogger(logger)
  # load config
  config <- yaml::read_yaml(configFilePath)
  connection <- DatabaseConnector::connect(config$cdmDataSource)

  if (createReferences) {
    ParallelLogger::logInfo("Creating and populating reference tables...")
    createReferenceTables(connection, config)
  }

  if (length(config$maintinedAtlasCohortList) & addDefaultAtlasCohorts) {
    ParallelLogger::logInfo(paste("removing atlas cohort", config$maintinedAtlasCohortList))
    removeAtlasCohort(connection, config, config$maintinedAtlasCohortList)
    ParallelLogger::logInfo("Adding maintained atlas cohorts from config")

    for (aid in config$maintinedAtlasCohortList) {
      ParallelLogger::logInfo(paste("adding cohort reference", aid))
      insertAtlasCohortRef(connection, config, aid)
    }
  }
 
  if (.createExposureCohorts) {
    ParallelLogger::logInfo("Creating exposure cohorts")
    createCohorts(connection, config)
  }
  
  if (.createOutcomeCohorts) {
    if(addDefaultAtlasCohorts) {
      for (aid in config$maintinedAtlasCohortList) {
        base::writeLines(paste("Generating custom outcome cohort", aid))
        addAtlasOutcomeCohort(connection, config, aid)
      }
    }

    ParallelLogger::logInfo("Creating outcome cohorts")
    createOutcomeCohorts(connection, config)
  }
  # generate summary tables
  if (.generateSummaryTables) {
    ParallelLogger::logInfo("Generating cohort summary tables")
    createSummaryTables(connection, config)
  }

  if (.runSCC) {
    ParallelLogger::logInfo("Generating fresh scc results tables")
    createResultsTables(connection, config)
    # run SCC
    for (dataSource in config$dataSources) {
      ParallelLogger::logInfo(paste("Running scc on", dataSource$database))
      batchScc(connection, config, dataSource)
    }
  }
  compileResults(connection, config)

  # TODO: run SCCS
}

addAtlasCohort <- function(configFilePath = "config/global-cfg.yml", atlasId, removeExisting = FALSE, logFileName = "rbAtlasCohort.log") {
  logger <- ParallelLogger::createLogger(
    name="SIMPLE",
    threshold="INFO",
    appenders = list(
      ParallelLogger::createFileAppender(layout = ParallelLogger::layoutTimestamp, fileName = logFileName ),
      ParallelLogger::createConsoleAppender(layout = ParallelLogger::layoutTimestamp)
    )
  )
  ParallelLogger::registerLogger(logger)
  config <- yaml::read_yaml(configFilePath)
  connection <- DatabaseConnector::connect(config$cdmDataSource)
  if (removeExisting) {
    ParallelLogger::logInfo("Removing existing cohort")
    removeAtlasCohort(connection, config, atlasId) # tested and works
  }
  
  ParallelLogger::logInfo("Inserting references")
  insertAtlasCohortRef(connection, config, atlasId) # tested and works
  
  ParallelLogger::logInfo("Running cohort")
  # Removes then adds cohort with atlas generated sql
  addAtlasOutcomeCohort(connection, config, atlasId) # tested and works
  
  ParallelLogger::logInfo("Adding summary")
  # Adds new cohort to summary table
  addOutcomeSummary(connection, config, atlasId) # untested
  
  for (dataSource in config$dataSources) {
    ParallelLogger::logInfo(paste("Getting scc", dataSource$database))
    generateCustomOutcomeResult(connection, config, dataSource, atlasId) # untested
  }
  
  ParallelLogger::logInfo(paste("Adding to final results table", dataSource$database))
  addAtlasResultsToMergedTable(connection, config, atlasId) # untested
}