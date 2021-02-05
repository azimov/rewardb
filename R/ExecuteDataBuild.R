#' .getLogger
#' @description
#' Get the logger and set where the log file is stored.
#' @param logFileName path to log file
#' @param .clearLoggers clear existing loggers and make new one
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

#' getCdmVersion
#' @description
#' Get the cdm version used (e.g. 5.3.1)
#' @param connection DatabaseConnector connection
#' @param config reward cdmConfig loaded with cdmConfig
getCdmVersion <- function(connection, config) {
  sql <- "SELECT cdm_version FROM @cdm_schema.cdm_source"
  DatabaseConnector::renderTranslateQuerySql(connection, sql, cdm_schema = config$cdmSchema)[[1]]
}

#' getCdmVersion
#' @description
#' Get the database version if a _version table exists. This is a JNJ standard but not a requirment of a cdm
#' Returns -1 where table is not present. Only really used for debugging in meta-data
#' @param connection DatabaseConnector connection
#' @param cdmConfig reward cdmConfig loaded with cdmConfig
getDatabaseId <- function(cdmConfig) {
  connection <- DatabaseConnector::connect(cdmConfig$connectionDetails)
  sql <- " SELECT version_id FROM @cdm_schema._version;"
  version <- -1
  tryCatch({
    version <- DatabaseConnector::renderTranslateQuerySql(connection, sql, cdm_schema = cdmConfig$cdmSchema)[[1]]
  },
  error = ParallelLogger::logError
  )
  DatabaseConnector::disconnect(connection)
  return(version)
}

#' getZippedSccResults
#' @description
#' Get zip files for scc
#' Partial reward execution with a subset of targets or outcomes. If both are null this will generate SCC results for all
#' exposure and outcome pairs. This is only really useful if you're adding an cohort after the full result set has been
#' generated.
#' @param config - cdm config loaded with loadCdmConfig function
#' @param connection DatabaseConnector connection
#' @param outcomeCohortIds - vector of outcome cohort ids or NULL
#' @param targetCohortIds - vector of exposure cohort ids or NULL
#' @param .generateCohortStats - generate time on treatment and time to outcome stats or not
#' @returns list of zip file locations
getZippedSccResults <- function(
  config,
  connection,
  configId,
  outcomeCohortIds = NULL,
  targetCohortIds = NULL,
  .generateCohortStats = TRUE
) {

  cdmVersion <- getCdmVersion(connection, config)
  dbId <- getDatabaseId(config)

  if (!dir.exists(configId)) {
    dir.create(configId)
  }

  tryCatch({
    getSccSettingsSql <- "SELECT * FROM @reference_schema.@analysis_setting WHERE type_id = 'scc' AND analysis_id = 1"
    sccAnalysisSettings <- DatabaseConnector::renderTranslateQuerySql(connection,
                                                                      getSccSettingsSql,
                                                                      reference_schema = config$referenceSchema,
                                                                      analysis_setting = config$tables$analysisSetting)

    zipFiles <- apply(sccAnalysisSettings, 1, function(analysis) {
      tableNames <- list()
      analysisId <- analysis[["ANALYSIS_ID"]]
      ParallelLogger::logInfo(paste("Generating scc results with setting id", analysisId))
      analysisSettings <- RJSONIO::fromJSON(rawToChar(base64enc::base64decode(analysis["OPTIONS"])))

      sccSummary <- runScc(connection, config, analysisId, analysisSettings, exposureIds = targetCohortIds, outcomeIds = outcomeCohortIds)
      dataFileName <- file.path(configId, paste0("rb-results-", config$database, "-aid-", analysisId, ".csv"))
      ParallelLogger::logInfo(paste("Writing file", dataFileName))
      suppressWarnings({ write.csv(sccSummary[names(rewardb::SCC_RESULT_COL_NAMES)], dataFileName, na = "", row.names = FALSE, fileEncoding = "ascii") })
      tableNames[[basename(dataFileName)]] <- "scc_result"

      if (.generateCohortStats) {
        timeOnTreatment <- getAverageTimeOnTreatment(config, analysisSettings, analysisId = analysisId, targetCohortIds = targetCohortIds, outcomeCohortIds = outcomeCohortIds)
        statsFileName <- file.path(configId, paste0("rb-results-", config$database, "-aid-", analysisId, "-time_on_treatment_stats", ".csv"))
        suppressWarnings({ write.csv(timeOnTreatment, statsFileName, na = "", row.names = FALSE, fileEncoding = "ascii") })
        tableNames[[basename(statsFileName)]] <- "time_on_treatment"
      }

      exportZipFile <- paste0(configId, "-reward-scc-results-aid-", config$database, "-", analysisId, ".zip")
      ParallelLogger::logInfo("Exporting results zip to ", exportZipFile)
      exportResults(config,
                    exportZipFile = exportZipFile,
                    tableNames = tableNames,
                    csvPattern = paste0("rb-results-", config$database, "-aid-", analysisId, "*.csv"),
                    cdmVersion = cdmVersion,
                    databaseId = dbId,
                    exportPath = configId)

      return(exportZipFile)
    })
  },
    error = ParallelLogger::logError
  )
  return(zipFiles)
}

#' Full rewardb execution
#' @description
#' Run a full execution of the rewardb pipeline on the CDM sets specified
#' This creates a set of reference tables, creates exposure cohorts, creates outcome cohorts, generates summary tables
#' of combined outcome, exposure pairs and runs SCC analysis.
#' if dataSources is set to a vector then the anlaysis will only be performed on this subset - these must be specified in the config yml file
#'
#' @param cdmConfigFilePath path on disk to a yaml configuration file containing all needed config setting to point at relevant cdm
#' @param .createReferences create the reference tables (set to false if already ran)
#' @param .addDefaultAtlasCohorts use default cohort ids speciefied in config file (these can be added later manually)
#' @param .createExposureCohorts Create the drug exposure cohorts default is TRUE
#' @param .createOutcomeCohorts Create the outcome cohorts, default is TRUE
#' @param .runSCC perform self controlled cohort analysis and add results to the merged output db
#' @param dataSources vector of strings or null - keys to cdm stores to use. By default all cdms are added for a given config file
#' @export
generateSccResults <- function(
  cdmConfigFilePath,
  .createExposureCohorts = TRUE,
  .createOutcomeCohorts = TRUE,
  .runSCC = TRUE,
  .generateCohortStats = TRUE,
  logFileName = "rbDataBuild.log"
) {
  logger <- .getLogger(logFileName)
  # load config
  config <- loadCdmConfig(cdmConfigFilePath)
  connection <- DatabaseConnector::connect(connectionDetails = config$connectionDetails)

  tryCatch({
    if (.createExposureCohorts) {
      ParallelLogger::logInfo("Creating exposure cohorts")
      createCohorts(connection, config)
    }

    if (.createOutcomeCohorts) {
      ParallelLogger::logInfo("Creating outcome cohorts")
      createOutcomeCohorts(connection, config)
    }
  },
    error = ParallelLogger::logError
  )

  if (.runSCC) {
    zipFiles <- getZippedSccResults(config,
                                    connection,
                                    configId = config$exportPath,
                                    outcomeCohortIds = NULL,
                                    targetCohortIds = NULL,
                                    .generateCohortStats = .generateCohortStats)
  }
  DatabaseConnector::disconnect(connection)
  ParallelLogger::unregisterLogger(logger)
  return(zipFiles)
}

#' oneOffSccResults
#' @description
#' Partial reward execution with a subset of targets or outcomes. If both are null this will generate SCC results for all
#' exposure and outcome pairs. This is only really useful if you're adding an cohort after the full result set has been
#' generated.
#' @param cdmConfigFilePath - path to cdm config loaded with loadCdmConfig function
#' @param configId - string id that will be used as a prefix to store results files
#' @param outcomeCohortIds - vector of outcome cohort ids or NULL
#' @param targetCohortIds - vector of exposure cohort ids or NULL
#' @param .generateCohortStats - generate time on treatment and time to outcome stats or not
#' @param getDbId - assumes CDM version is stored in the cdm_source table
#' @param logFileName logfile used. If null is based on the passed configId
oneOffSccResults <- function(
  cdmConfigFilePath,
  configId,
  outcomeCohortIds = NULL,
  targetCohortIds = NULL,
  .generateCohortStats = TRUE,
  logFileName = NULL
) {

  config <- loadCdmConfig(cdmConfigFilePath)
  if (is.null(logFileName)) {
    logFileName <- paste0(configId, "scc-data-build-results.log")
  }
  logger <- .getLogger(logFileName)
  connection <- DatabaseConnector::connect(connectionDetails = config$connectionDetails)
  zipFiles <- getZippedSccResults(config,
                                  connection,
                                  configId,
                                  outcomeCohortIds = outcomeCohortIds,
                                  targetCohortIds = targetCohortIds,
                                  .generateCohortStats = .generateCohortStats)
  DatabaseConnector::disconnect(connection)
  ParallelLogger::unregisterLogger(logger)

  return(zipFiles)
}