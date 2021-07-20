#' For patients exposed to outcomes, compute feature extraction stats
runExposedOutcomeFetaureExtraction <- function(connectionDetails,
                                               exposureIds,
                                               outcomeIds,
                                               outcomeDatabaseSchema,
                                               exposureDatabaseSchema,
                                               cdmDatabaseSchema,
                                               outcomeTable = "condition_era",
                                               exposureTable = "drug_era",
                                               oracleTempSchema = NULL,
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
                                               covariateSettings = NULL) {
  if (riskWindowEndExposed < riskWindowStartExposed && !addLengthOfExposureExposed)
    stop("Risk window end (exposed) should be on or after risk window start")
  if (riskWindowEndUnexposed < riskWindowStartUnexposed && !addLengthOfExposureUnexposed)
    stop("Risk window end (unexposed) should be on or after risk window start")

  start <- Sys.time()
  exposureTable <- tolower(exposureTable)
  outcomeTable <- tolower(outcomeTable)
  if (exposureTable == "drug_era") {
    exposureStartDate <- "drug_era_start_date"
    exposureEndDate <- "drug_era_end_date"
    exposureId <- "drug_concept_id"
    exposurePersonId <- "person_id"
  } else if (exposureTable == "drug_exposure") {
    exposureStartDate <- "drug_exposure_start_date"
    exposureEndDate <- "drug_exposure_end_date"
    exposureId <- "drug_concept_id"
    exposurePersonId <- "person_id"
  } else {
    exposureStartDate <- "cohort_start_date"
    exposureEndDate <- "cohort_end_date"

    exposureId <- "cohort_definition_id"

    exposurePersonId <- "subject_id"
  }

  if (outcomeTable == "condition_era") {
    outcomeStartDate <- "condition_era_start_date"
    outcomeId <- "condition_concept_id"
    outcomePersonId <- "person_id"
  } else if (outcomeTable == "condition_occurrence") {
    outcomeStartDate <- "condition_start_date"
    outcomeId <- "condition_concept_id"
    outcomePersonId <- "person_id"
  } else {
    outcomeStartDate <- "cohort_start_date"
    outcomeId <- "cohort_definition_id"
    outcomePersonId <- "subject_id"
  }

  conn <- DatabaseConnector::connect(connectionDetails)
  on.exit(DatabaseConnector::disconnect(conn))

  ParallelLogger::logInfo("Constructing time at risk windows and temporary cohorts")


  if (length(outcomeIds)) {
    DatabaseConnector::insertTable(connection = conn,
                                   tableName = "#scc_outcome_ids",
                                   data = data.frame(outcome_id = outcomeIds),
                                   tempTable = TRUE)
  }

  if (length(exposureIds)) {
    DatabaseConnector::insertTable(connection = conn,
                                   tableName = "#scc_exposure_ids",
                                   data = data.frame(exposure_id = exposureIds),
                                   tempTable = TRUE)
  }
  sql <- SqlRender::readSql(system.file("sql/sql_server", "exposureOutcomeCohort.sql", package = "rewardb"))
  DatabaseConnector::renderTranslateExecuteSql(connection = conn,
                                               sql = sql,
                                               oracleTempSchema = oracleTempSchema,
                                               cdm_database_schema = cdmDatabaseSchema,
                                               exposure_ids = exposureIds,
                                               outcome_ids = outcomeIds,
                                               exposure_database_schema = exposureDatabaseSchema,
                                               exposure_table = exposureTable,
                                               exposure_start_date = exposureStartDate,
                                               exposure_end_date = exposureEndDate,
                                               exposure_id = exposureId,
                                               exposure_person_id = exposurePersonId,
                                               outcome_database_schema = outcomeDatabaseSchema,
                                               outcome_table = outcomeTable,
                                               outcome_start_date = outcomeStartDate,
                                               outcome_id = outcomeId,
                                               outcome_person_id = outcomePersonId,
                                               first_exposure_only = firstExposureOnly,
                                               first_outcome_only = firstOutcomeOnly,
                                               min_age = minAge,
                                               max_age = maxAge,
                                               study_start_date = studyStartDate,
                                               study_end_date = studyEndDate,
                                               add_length_of_exposure_exposed = addLengthOfExposureExposed,
                                               risk_window_start_exposed = riskWindowStartExposed,
                                               risk_window_end_exposed = riskWindowEndExposed,
                                               add_length_of_exposure_unexposed = addLengthOfExposureUnexposed,
                                               risk_window_end_unexposed = riskWindowEndUnexposed,
                                               risk_window_start_unexposed = riskWindowStartUnexposed,
                                               has_full_time_at_risk = hasFullTimeAtRisk,
                                               washout_window = washoutPeriod,
                                               followup_window = followupPeriod)

  if (is.null(covariateSettings)) {
    covariateSettings <- FeatureExtraction::createDefaultCovariateSettings()
  }

  covariateData <- list()

  for (exposureCohortId in exposureIds) {
    for (outcomeCohortId in outcomeIds) {
      uid <- paste0(exposureCohortId, '-', outcomeCohortId)
      covariateData[[uid]] <- FeatureExtraction::getDbCovariateData(connectionDetails = connectionDetails,
                                                             cdmDatabaseSchema = cdmDatabaseSchema,
                                                             cohortDatabaseSchema = exposureDatabaseSchema,
                                                             cohortTable = "temp_result_cohorts",
                                                             cohortId = exposureCohortId * 100000 + outcomeCohortId,
                                                             cohortTableIsTemp = FALSE,
                                                             rowIdField = "subject_id",
                                                             covariateSettings = covariateSettings)
    }
  }
  DatabaseConnector::renderTranslateExecuteSql(conn, "TRUNCATE TABLE @cohort_schema.temp_result_cohorts; DROP TABLE @cohort_schema.temp_result_cohorts", cohort_schema = exposureDatabaseSchema)
  delta <- Sys.time() - start
  ParallelLogger::logInfo(paste("Computing covariate features took", signif(delta, 3), attr(delta, "units")))
  return(covariateData)
}


#' @title
#' Get Self-Controlled cohort stats
#' @description
#' compute average time on treatement and average time to outcome
#' @param config reward cdm config object
#' @param analysisOptions arguments for SCC
#' @param analysisId Id for storage in returned datafame
#' @param targetCohortIds target cohort ids
#' @param outcomeCohortIds outcome cohort ids
#' @return data.frame of stats mapped for reward db
getExposedOutcomeFetaures <- function(config, analysisOptions = list(), analysisId = NULL, exposureIds = NULL, outcomeIds = NULL, covariateSettings = NULL) {
  args <- list(
    connectionDetails = config$connectionDetails,
    cdmDatabaseSchema = config$cdmSchema,
    outcomeDatabaseSchema = config$resultSchema,
    exposureDatabaseSchema = config$resultSchema,
    exposureIds = exposureIds,
    outcomeIds = outcomeIds,
    exposureTable = config$tables$cohort,
    outcomeTable = config$tables$outcomeCohort,
    covariateSettings = covariateSettings
  )

  results <- do.call(runExposedOutcomeFetaureExtraction, c(args, analysisOptions))
  
  return(results)
}