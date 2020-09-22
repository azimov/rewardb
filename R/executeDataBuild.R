getLogger <- function(logFileName, .clearLoggers = TRUE) {
  if (.clearLoggers) {
    ParallelLogger::clearLoggers()
  }
  logger <- ParallelLogger::createLogger(
    name = "SIMPLE",
    threshold = "INFO",
    appenders = list(
      ParallelLogger::createFileAppender(layout = ParallelLogger::layoutTimestamp, fileName = logFileName),
      ParallelLogger::createConsoleAppender(layout = ParallelLogger::layoutTimestamp)
    )
  )
  ParallelLogger::registerLogger(logger)
  return(logger)
}

checkDataSources <- function(dataSources, config) {
  if (is.null(dataSources)) {
    dataSources <- names(config$dataSources)
  }

  dsVals <- list()
  for (ds in dataSources) {
    if (!(ds %in% names(config$dataSources))) {
      errorMsg <- paste(ds, "not found in configuration")
      ParallelLogger::logError(errorMsg)
      stop(errorMsg)
    }

    dsVals[[ds]] <- config$dataSources[[ds]]
  }

  return(dsVals)
}

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
  logger <- getLogger(logFileName)
  # load config
  config <- yaml::read_yaml(configFilePath)
  connection <- DatabaseConnector::connect(config$cdmDataSource)
  dataSources <- checkDataSources(dataSources, config)

  tryCatch(
    {
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

    getDataFileName <- function(dataSource) {
        paste0(config$exportPath, "/scc-results-full-", dataSource$database, ".csv")
    }

    if (.runSCC) {
      ParallelLogger::logInfo("Generating fresh scc results tables")
      # run SCC
      if (!dir.exists(config$exportPath)) {
        dir.create(config$exportPath)
      }

      tableNames <- list()
      for (dataSource in dataSources) {
        createResultsTable(connection, config, dataSource)
        sccSummary <- runScc(connection, config, dataSource)
        dataFileName <- getDataFileName(dataSource)
        ParallelLogger::logInfo(paste("Writing file", dataFileName))
        write.csv(sccSummary[names(rewardb::SCC_RESULT_COL_NAMES)], dataFileName, row.names = FALSE, na="")
        tableNames[[basename(dataFileName)]] <- "scc_result"
      }

      ParallelLogger::logInfo("Exporting results zip")
      exportResults(config, tableNames = tableNames, csvPattern = "scc-results-full-*.csv")
    }

  },
    error = ParallelLogger::logError,
    finally = function() {
      DatabaseConnector::disconnect(connection)
      ParallelLogger::unregisterLogger(logger)
  })
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
  logFileName = "rbAtlasCohort.log",
  dataDir = "data",
  addResult = TRUE
) {
  logger <- getLogger(logFileName)
  # load config
  config <- yaml::read_yaml(configFilePath)
  connection <- DatabaseConnector::connect(config$cdmDataSource)
  dataSources <- checkDataSources(dataSources, config)
  tryCatch(
    {

      if (removeExisting) {
        ParallelLogger::logInfo("Removing existing cohort")
        removeAtlasCohort(connection, config, atlasId, names(dataSources)) # tested and works
      }

      ParallelLogger::logInfo("Inserting references")
      insertAtlasCohortRef(connection, config, atlasId) # tested and works

      ParallelLogger::logInfo("Running cohort")
      # Removes then adds cohort with atlas generated sql
      addAtlasOutcomeCohort(connection, config, atlasId, names(dataSources)) # tested and works

      ParallelLogger::logInfo("Adding summary")
      # Adds new cohort to summary table
      addOutcomeSummary(connection, config, atlasId, names(dataSources))

      getDataFileName <- function(dataSource) {
        paste0(dataDir, "/scc-results-", atlasId, "-", dataSource$database, ".csv")
      }

      for (ds in names(dataSources)) {
        dataSource <- config$dataSources[[ds]]
        ParallelLogger::logInfo(paste("Getting scc", dataSource$database))
        sccSummary <- runScc(connection = connection, config = config, dataSource = dataSource, outcomeIds = atlasId, storeResults = FALSE)
        dataFileName <- getDataFileName(dataSource)
        ParallelLogger::logInfo(paste("Saving result", dataFileName))
        write.csv(sccSummary, dataFileName, row.names = FALSE)
      }

      ParallelLogger::logInfo(paste("Adding to final results table", dataSource$database))

      if (addResult) {
        results <- data.frame()

        # Merge data files and append them to the table
        for (ds in names(dataSources)) {
          dataSource <- config$dataSources[[ds]]
          dataFileName <- getDataFileName(dataSource)
          ParallelLogger::logInfo(paste("Adding result to merged table", dataFileName))
          if (file.exists(dataFileName)) {
            results <- rbind(results, read.csv(dataFileName))
          }
        }

        if (length(results)) {
          addCsvAtlasResultsToMergedTable(connection = connection, config = config, results = results, removeOutcomeIds = TRUE)
        }
      }
    },
    error = ParallelLogger::logError,
    finally = function() {
      DatabaseConnector::disconnect(connection)
      ParallelLogger::unregisterLogger(logger)
    }
  )
}

#' Add custom exposure cohort
#' @description
#' Add custom atlas cohorts to the results dataset. Just needs an atlasId and cdm and the cohort will be generated with the
#' result being added to the final dataset.
#'
#' @param conceptSetId add specified atlas id
#' @param removeExisting removes any already existing data (useful if you change the cohort definition in atlas as rewardb does not track definition verions)
#' @param configFilePath path on disk to a yaml configuration file containing all needed config setting to point at relevant cdm
#' @param dataSources vector of strings or null - keys to cdm stores to use. By default all cdms are added for a given config file
#' @param logFileName location to store parallel logger logfile

addCustomExposureConcept <- function(
  conceptSetId,
  cohortName,
  configFilePath = "config/global-cfg.yml",
  removeExisting = FALSE,
  dataSources = NULL,
  logFileName = "rbAtlasCohort.log",
  outputPath = "data",
  addResult = TRUE
) {
    logger <- getLogger(logFileName)
  # load config
  config <- yaml::read_yaml(configFilePath)
  connection <- DatabaseConnector::connect(config$cdmDataSource)
  dataSources <- checkDataSources(dataSources, config)

  tryCatch(
    {
      conceptSetDefinition <- ROhdsiWebApi::getConceptSetDefinition(conceptSetId, config$webApiUrl)

      if (!dir.exists(outputPath)) {
        dir.create(outputPath)
      }

      getDataFileName <- function(dataSourceKey) {
        paste0(outputPath, "/", dataSourceKey, "-SCC-CustomExposure", conceptSetId, ".csv")
      }

      ParallelLogger::logInfo("Adding for data sources", paste(names(dataSources)))

      connection <- DatabaseConnector::connect(config$cdmDataSource)
      if (removeExisting) {
        ParallelLogger::logInfo("Removing existing cohort")
        removeCustomExposureCohort(connection, config, conceptSetId, dataSource)
      }

      ParallelLogger::logInfo("Inserting references")
      insertCustomExposureRef(connection, config, conceptSetId, conceptSetDefinition, cohortName)

      for (dataSourceKey in names(dataSources)) {
        dataSource <- dataSources[[dataSourceKey]]
        ParallelLogger::logInfo("Running cohort")
        # Removes then adds cohort with atlas generated sql
        addCustomExposureCohort(connection, config, conceptSetId, dataSource)

        ParallelLogger::logInfo(paste("Getting scc", dataSource$database))
        dataFileName <- getDataFileName(dataSourceKey)
        sccSummary <- runScc(connection = connection, config = config, dataSource = dataSource, exposureIds = conceptSetId, storeResults = FALSE)
        write.csv(sccSummary, dataFileName, row.names = FALSE)
      }

      if (addResult) {
        results <- data.frame()

        # Merge data files and append them to the table
        for (dataSourceKey in names(dataSources)) {
          dataFileName <- getDataFileName(dataSourceKey)
          ParallelLogger::logInfo(paste("Adding result to merged table", dataFileName))
          if (file.exists(dataFileName)) {
            results <- rbind(results, read.csv(dataFileName))
          }
        }

        if (length(results)) {
          addCsvAtlasResultsToMergedTable(connection = connection, config = config, results = results, removeOutcomeIds = TRUE)
        }
      }

    },
    error = ParallelLogger::logError,
    finally = function() {
      DatabaseConnector::disconnect(connection)
      ParallelLogger::unregisterLogger(logger)
    }
  )
}
