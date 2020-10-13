.getLogger <- function(logFileName, .clearLoggers = TRUE) {
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


#' Full rewardb execution
#' @description
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
  configFilePath,
  .createExposureCohorts = TRUE,
  .createOutcomeCohorts = TRUE,
  .generateSummaryTables = TRUE,
  .runSCC = TRUE,
  dataSources = NULL,
  logFileName = "rbDataBuild.log"
) {
  logger <- .getLogger(logFileName)
  # load config
  config <- yaml::read_yaml(configFilePath)
  connection <- DatabaseConnector::connect(config$cdmDataSource)
  dataSources <- .checkDataSources(dataSources, config)

  tryCatch(
    {

    if (.createExposureCohorts) {
      ParallelLogger::logInfo("Creating exposure cohorts")
      createCohorts(connection, config)
    }

    if (.createOutcomeCohorts) {

      ParallelLogger::logInfo("Creating outcome cohorts")
      createOutcomeCohorts(connection, config)
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