#' Run a full execution of the rewardb pipeline on the CDM sets specified
#' This creates a set of reference tables, creates exposure cohorts, creates outcome cohorts, generates summary tables
#' of combined outcome, exposure pairs and runs SCC analysis.
#' if dataSources is set to a vector then the anlaysis will only be performed on this subset - these must be specified in the config yml file
#'
#' @param configFilePath path on disk to a yaml configuration file containing all needed config setting to point at relevant cdm
#' @param .createReferences create the reference tables (set to false if already ran)
#' @param .addDefaultAtlasCohorts use default cohort ids speciefied in config file (these can be added later manually)
#' @param .createExposureCohorts Create the drug exposure cohorts default is TRUE
#' @param .createOutcomeCohorts Create the outcome cohorts, default is TRUE
#' @param .generateSummaryTables Generates summaries for outcome, target pairs
#' @param .runSCC perform self controlled cohort analysis and add results to the merged output db
#' @param dataSources vector of strings or null - keys to cdm stores to use. By default all cdms are added for a given config file
#' @export
fullExecution <- function(
  configFilePath = "config/global-cfg.yml",
  .createReferences = TRUE,
  .addDefaultAtlasCohorts = TRUE,
  .createExposureCohorts = TRUE,
  .createOutcomeCohorts = TRUE,
  .generateSummaryTables = TRUE,
  .runSCC = TRUE,
  dataSources = NULL,
  logFileName = "rbDataBuild.log"
) {
  logger <- ParallelLogger::createLogger(
    name = "DataGen",
    threshold = "INFO",
    appenders = list(
      ParallelLogger::createFileAppender(layout = ParallelLogger::layoutTimestamp, fileName = logFileName),
      ParallelLogger::createConsoleAppender(layout = ParallelLogger::layoutTimestamp)
    )
  )
  ParallelLogger::registerLogger(logger)
  # load config
  config <- yaml::read_yaml(configFilePath)
  connection <- DatabaseConnector::connect(config$cdmDataSource)

  if(is.null(dataSources)) {
    dataSources <- names(config$dataSources)
  }

  for (ds in dataSources) {
    if (!(ds %in% names(config$dataSources) )) {
      errorMsg <- paste(ds, "not found in configuration")
      ParallelLogger::logError(errorMsg)
      stop(errorMsg)
    }
  }

  if (.createReferences) {
    ParallelLogger::logInfo("Creating and populating reference tables...")
    createReferenceTables(connection, config, dataSources)
  }

  if (length(config$maintinedAtlasCohortList) & .addDefaultAtlasCohorts) {
    ParallelLogger::logInfo(paste("removing atlas cohort", config$maintinedAtlasCohortList))
    removeAtlasCohort(connection, config, config$maintinedAtlasCohortList, dataSources)
    ParallelLogger::logInfo("Adding maintained atlas cohorts from config")

    for (aid in config$maintinedAtlasCohortList) {
      ParallelLogger::logInfo(paste("adding cohort reference", aid))
      insertAtlasCohortRef(connection, config, aid)
    }
  }

  if (.createExposureCohorts) {
    ParallelLogger::logInfo("Creating exposure cohorts")
    createCohorts(connection, config, dataSources)
  }

  if (.createOutcomeCohorts) {
    if (.addDefaultAtlasCohorts) {
      for (aid in config$maintinedAtlasCohortList) {
        base::writeLines(paste("Generating custom outcome cohort", aid))
        addAtlasOutcomeCohort(connection, config, aid, dataSources)
      }
    }

    ParallelLogger::logInfo("Creating outcome cohorts")
    createOutcomeCohorts(connection, config, dataSources)
  }
  # generate summary tables
  if (.generateSummaryTables) {
    ParallelLogger::logInfo("Generating cohort summary tables")
    createSummaryTables(connection, config, dataSources)
  }

  if (.runSCC) {
    ParallelLogger::logInfo("Generating fresh scc results tables")
    createResultsTables(connection, config, dataSources)
    # run SCC
    for (ds in dataSources) {
      dataSource <- config$dataSources[[ds]]
      batchScc(connection, config, dataSource)
    }
  }
  compileResults(connection, config, dataSources)
}

#' Add custom atlas cohorts to the results dataset. Just needs an atlasId and cdm and the cohort will be generated with the
#' result being added to the final dataset.
#'
#' @param atlasId add specified atlas id
#' @param removeExisting removes any already existing data (useful if you change the cohort definition in atlas as rewardb does not track definition verions)
#' @param configFilePath path on disk to a yaml configuration file containing all needed config setting to point at relevant cdm
#' @param dataSources vector of strings or null - keys to cdm stores to use. By default all cdms are added for a given config file
#' @param logFileName location to store parallel logger logfile
addAtlasCohort <- function(
  atlasId,
  configFilePath = "config/global-cfg.yml",
  removeExisting = FALSE,
  dataSources = NULL,
  logFileName = "rbAtlasCohort.log"
) {
  logger <- ParallelLogger::createLogger(
    name = "SIMPLE",
    threshold = "INFO",
    appenders = list(
      ParallelLogger::createFileAppender(layout = ParallelLogger::layoutTimestamp, fileName = logFileName),
      ParallelLogger::createConsoleAppender(layout = ParallelLogger::layoutTimestamp)
    )
  )
  ParallelLogger::registerLogger(logger)
  config <- yaml::read_yaml(configFilePath)
  connection <- DatabaseConnector::connect(config$cdmDataSource)

  if(is.null(dataSources)) {
    dataSources <- names(config$dataSources)
  }

  for (ds in dataSources) {
    if (!(ds %in% names(config$dataSources) )) {
      errorMsg <- paste(ds, "not found in configuration")
      ParallelLogger::logError(errorMsg)
      stop(errorMsg)
    }
  }

  if (removeExisting) {
    ParallelLogger::logInfo("Removing existing cohort")
    removeAtlasCohort(connection, config, atlasId, dataSources) # tested and works
  }

  ParallelLogger::logInfo("Inserting references")
  insertAtlasCohortRef(connection, config, atlasId) # tested and works

  ParallelLogger::logInfo("Running cohort")
  # Removes then adds cohort with atlas generated sql
  addAtlasOutcomeCohort(connection, config, atlasId, dataSources) # tested and works

  ParallelLogger::logInfo("Adding summary")
  # Adds new cohort to summary table
  addOutcomeSummary(connection, config, atlasId, dataSources)

  for (ds in dataSources) {
    dataSource <- config$dataSources[[ds]]
    ParallelLogger::logInfo(paste("Getting scc", dataSource$database))
    generateCustomOutcomeResult(connection, config, dataSource, atlasId) # untested
  }

  ParallelLogger::logInfo(paste("Adding to final results table", dataSource$database))
  addAtlasResultsToMergedTable(connection, config, atlasId, dataSources)
}