#' @title
#' get logger
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


cleanUpSccDf <- function(data, sourceId, analysisId) {
  data <- base::do.call(data.frame, lapply(data, function(x) replace(x, is.infinite(x) | is.nan(x), NA)))
  data$source_id <- sourceId
  data$analysis_id <- as.integer(analysisId)
  data <- data %>%
    dplyr::rename("target_cohort_id" = "exposureId",
                  "outcome_cohort_id" = "outcomeId",
                  "rr" = "irr",
                  "c_at_risk" = "numPersons",
                  "se_log_rr" = "seLogRr",
                  "log_rr" = "logRr",
                  "t_at_risk" = "numPersons",
                  "t_pt" = "timeAtRiskExposed",
                  "t_cases" = "numOutcomesExposed",
                  "c_cases" = "numOutcomesUnexposed",
                  "c_pt" = "timeAtRiskUnexposed",
                  "lb_95" = "irrLb95",
                  "ub_95" = "irrUb95",
                  "p_value" = "p",
                  "num_exposures" = "numExposures")
  return(data)
}

#' @title
#' Get Zipped Scc Results
#' @description
#' Get zip files for scc
#' Partial reward execution with a subset of targets or outcomes. If both are null this will generate SCC results for all
#' exposure and outcome pairs. This is only really useful if you're adding an cohort after the full result set has been
#' generated.
#' @param cdmConfig cdm config loaded with loadCdmConfig function
#' @param connection DatabaseConnector connection
#' @param outcomeCohortIds - vector of outcome cohort ids or NULL
#' @param targetCohortIds - vector of exposure cohort ids or NULL
#' @param .generateCohortStats - generate time on treatment and time to outcome stats or not
#'
#' @importFrom vroom vroom_write
#' @returns list of zip file locations
computeSccResults <- function(cdmConfig,
                              globalConfig,
                              connection,
                              exportPath,
                              postProcessFunction = NULL,
                              postProcessArgs = list(),
                              analysisIds = NULL,
                              outcomeCohortIds = NULL,
                              targetCohortIds = NULL) {

  getSccSettingsSql <- "SELECT * FROM @reference_schema.@analysis_setting WHERE type_id = 'scc'
  {@analysis_ids != ''} ? {AND analysis_id IN (@analysis_ids)}"
  sccAnalysisSettings <- DatabaseConnector::renderTranslateQuerySql(connection,
                                                                    getSccSettingsSql,
                                                                    analysis_ids = analysisIds,
                                                                    reference_schema = cdmConfig$referenceSchema,
                                                                    analysis_setting = cdmConfig$tables$analysisSetting)

  apply(sccAnalysisSettings, 1, function(analysis) {
    analysisId <- analysis[["ANALYSIS_ID"]]

    if (is.null(postProcessFunction)) {
      if (!dir.exists(exportPath)) {
        dir.create(exportPath)
      }

      if (cdmConfig$performActiveDataTransfer) {
        # Transfer dataset directly to reward database
        postProcessFunction <- function(sccBatch, position, cdmConfig, globalConfig, analysisId, exportPath) {
          ParallelLogger::logDebug("Pusing analysis id ", analysisId, "to database at postition ", position)
          if (nrow(sccBatch) == 0) {
            ParallelLogger::logError("Empty Data file:",
                                     analysisId,
                                     "PID:", position)
            return(sccBatch)
          }
          sccBatch <- cleanUpSccDf(sccBatch, cdmConfig$sourceId, analysisId)
          tableName <- ifelse(is.null(globalConfig$sccResultTransferTable),
                              "scc_result",
                              globalConfig$sccResultTransferTable)
          fileName <- paste0("scc-results-", cdmConfig$database, "-aid-", analysisId, "-position-", position, ".csv")
          dataFileName <- file.path(exportPath, fileName)
          ParallelLogger::logInfo("Writing ", dataFileName)
          # Note, entire data set is not written to a single file!
          vroom::vroom_write(sccBatch, dataFileName, delim = ",", na = "", append = FALSE)
          tryCatch({
            pgCopy(globalConfig$connectionDetails, dataFileName, globalConfig$rewardbResultsSchema, tableName)
            unlink(dataFileName)
          }, error = function(error, ...) {
            ParallelLogger::logError("Error committing batch to database, writing data to disk AID: ",
                                     analysisId,
                                     "PID: ", position)
            ParallelLogger::logError(error)
          })
          return(sccBatch)
        }

        postProcessArgs <- list(cdmConfig = cdmConfig,
                                analysisId = analysisId,
                                exportPath = exportPath,
                                globalConfig = globalConfig)
      } else {
        # By default, export analysis results to a csv file in batches (assumes the data set is large)
        postProcessFunction <- function(sccBatch, position, cdmConfig, analysisId, exportPath) {
          if (nrow(sccBatch) > 0) {
            sccBatch <- cleanUpSccDf(sccBatch, cdmConfig$sourceId, analysisId)
            dataFileName <- file.path(exportPath, paste0("scc-results-", cdmConfig$database, "-aid-", analysisId, ".csv"))
            vroom::vroom_write(sccBatch, dataFileName, delim = ",", na = "", append = position != 1)
          }
          return(sccBatch)
        }

        postProcessArgs <- list(cdmConfig = cdmConfig, analysisId = analysisId, exportPath = exportPath)
      }
    }
    ParallelLogger::logInfo(paste("Generating scc results with setting id", analysisId))
    analysisSettings <- RJSONIO::fromJSON(rawToChar(base64enc::base64decode(analysis["OPTIONS"])))
    runScc(postProcessFunction,
           postProcessArgs,
           cdmConfig,
           globalConfig,
           analysisId,
           analysisSettings,
           exposureIds = targetCohortIds,
           outcomeIds = outcomeCohortIds)
  })
}


#' @title
#' Full reward execution
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
generateSccResults <- function(cdmConfigFilePath,
                               globalConfig,
                               analysisIds = NULL,
                               outcomeCohortIds = NULL,
                               targetCohortIds = NULL,
                               logFileName = "rbDataBuild.log") {
  logger <- .getLogger(logFileName)
  # load config
  cdmConfig <- loadCdmConfiguration(cdmConfigFilePath)
  connection <- DatabaseConnector::connect(connectionDetails = cdmConfig$connectionDetails)
  on.exit(DatabaseConnector::disconnect(connection), add = TRUE)
  computeSccResults(cdmConfig,
                    globalConfig,
                    connection,
                    analysisIds = analysisIds,
                    exportPath = cdmConfig$exportPath,
                    outcomeCohortIds = outcomeCohortIds,
                    targetCohortIds = targetCohortIds)
  ParallelLogger::unregisterLogger(logger)
}


#' @title
#' oneOffSccResults
#' @description
#' Partial reward execution with a subset of targets or outcomes. If both are null this will generate SCC results for all
#' exposure and outcome pairs. This is only really useful if you're adding an cohort after the full result set has been
#' generated.
#' @param cdmConfigPath - path to cdm config loaded with loadCdmConfig function
#' @param configId - string id that will be used as a prefix to store results files
#' @param outcomeCohortIds - vector of outcome cohort ids or NULL
#' @param targetCohortIds - vector of exposure cohort ids or NULL
#' @param .generateCohortStats - generate time on treatment and time to outcome stats or not
#' @param getDbId - assumes CDM version is stored in the cdm_source table
#' @param logFileName logfile used. If null is based on the passed configId
runAdHocScc <- function(cdmConfigPath,
                        globalConfig,
                        configId,
                        outcomeCohortIds = NULL,
                        targetCohortIds = NULL,
                        logFileName = NULL) {

  cdmConfig <- loadCdmConfiguration(cdmConfigPath)
  if (is.null(logFileName)) {
    logFileName <- paste0(configId, "-scc-data-build-results.log")
  }
  logger <- .getLogger(logFileName)
  connection <- DatabaseConnector::connect(connectionDetails = cdmConfig$connectionDetails)
  on.exit(DatabaseConnector::disconnect(connection))
  computeSccResults(cdmConfig,
                    globalConfig,
                    connection,
                    exportPath = configId,
                    outcomeCohortIds = outcomeCohortIds,
                    targetCohortIds = targetCohortIds)
  ParallelLogger::unregisterLogger(logger)
}

#' @title
#' run Databuild Job
#' @description
#' Run analysis job script in rstudio session, this won't block the rstudio thread so other tasks can be completed
#' NOTE: that this copies the global env (and the values passed will modify .GlobalEnv variables of the same name)
#'
#' @param Charachter cdmConfigPath path to cdm configuration file
#' @param Charachter globalConfigPath path to rewarb global configuration
#'
#' @importFrom rstudioapi jobRunScript
#' @export
runDatabuildJob <- function(cdmConfigPath, globalConfigPath, rewardReferenceZipPath, name, workingDir = ".") {
  scriptPath <- system.file("scripts/runAnalysisJob.R", package = "rewardb")
  .GlobalEnv$cdmConfigPath <- normalizePath(cdmConfigPath)
  .GlobalEnv$globalConfigPath <- normalizePath(globalConfigPath)
  .GlobalEnv$rewardReferenceZipPath <- normalizePath(rewardReferenceZipPath)
  rstudioapi::jobRunScript(scriptPath, name = name, workingDir = workingDir, importEnv = TRUE)
}