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
#' @param cdmConfigFilePath path on disk to a yaml configuration file containing all needed config setting to point at relevant cdm
#' @param .createReferences create the reference tables (set to false if already ran)
#' @param .addDefaultAtlasCohorts use default cohort ids speciefied in config file (these can be added later manually)
#' @param .createExposureCohorts Create the drug exposure cohorts default is TRUE
#' @param .createOutcomeCohorts Create the outcome cohorts, default is TRUE
#' @param .generateSummaryTables Generates summaries for outcome, target pairs
#' @param .runSCC perform self controlled cohort analysis and add results to the merged output db
#' @param dataSources vector of strings or null - keys to cdm stores to use. By default all cdms are added for a given config file
#' @export
generateSccResults <- function(
  cdmConfigFilePath,
  .createExposureCohorts = TRUE,
  .createOutcomeCohorts = TRUE,
  .generateSummaryTables = TRUE,
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
    # generate summary tables
    if (.generateSummaryTables) {
      ParallelLogger::logInfo("Generating cohort summary tables")
      #createSummaryTables(connection, config, dataSources)
    }

    if (.runSCC) {
      if (!dir.exists(config$exportPath)) {
        dir.create(config$exportPath)
      }

      getSccSettingsSql <- "SELECT * FROM @reference_schema.@analysis_setting WHERE type_id = 'scc'"
      sccAnalysisSettings <- DatabaseConnector::renderTranslateQuerySql(connection,
                                                                        getSccSettingsSql,
                                                                        reference_schema = config$referenceSchema,
                                                                        analysis_setting = config$tables$analysisSetting)

      apply(sccAnalysisSettings, 1, function(analysis) {
        tableNames <- list()
        analysisId <- analysis[["ANALYSIS_ID"]]
        ParallelLogger::logInfo(paste("Generating scc results with setting id", analysisId))
        analysisSettings <- RJSONIO::fromJSON(rawToChar(base64enc::base64decode(analysis["OPTIONS"])))

        sccSummary <- runScc(connection, config, analysisId, analysisSettings)
        dataFileName <- file.path(config$exportPath, paste0("rb-results-", config$database, "-aid-", analysisId, ".csv"))
        ParallelLogger::logInfo(paste("Writing file", dataFileName))
        suppressWarnings({write.csv(sccSummary[names(rewardb::SCC_RESULT_COL_NAMES)], dataFileName, na = "", row.names = FALSE, fileEncoding = "ascii")})
        tableNames[[basename(dataFileName)]] <- "scc_result"

        if (.generateCohortStats) {
          timeOnTreatment <- getAverageTimeOnTreatment(connection, config, analysisSettings, analysisId = analysisId)
          statsFileName <- file.path(config$exportPath, paste0("rb-results-", config$database, "-aid-", analysisId, "-time_on_treatment_stats", ".csv"))
          suppressWarnings({write.csv(timeOnTreatment, statsFileName, na = "", row.names = FALSE, fileEncoding = "ascii")})
          tableNames[[basename(statsFileName)]] <- "time_on_treatment"
        }

        exportZipFile <- paste0("reward-b-scc-results-aid-", config$database, "-", analysisId,".zip")
        ParallelLogger::logInfo("Exporting results zip to", exportZipFile)
        exportResults(config, exportZipFile = exportZipFile, tableNames = tableNames, csvPattern = paste0("rb-results-", config$database, "-aid-", analysisId, "*.csv"))
      })
    }
  },
  error = ParallelLogger::logError
  )
  DatabaseConnector::disconnect(connection)
  ParallelLogger::unregisterLogger(logger)

}