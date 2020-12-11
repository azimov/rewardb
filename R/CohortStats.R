# Compute the average time on treatement for cohort pairs
getAverageTimeOnTreatment <- function(connection, config, analysisOptions = list(), analysisId = NULL, targetCohortIds = NULL, outcomeCohortIds = NULL) {
  args <- list(
    connection = connection,
    cdmDatabaseSchema = config$cdmSchema,
    outcomeDatabaseSchema = config$resultSchema,
    exposureDatabaseSchema = config$resultSchema,
    exposureIds = targetCohortIds,
    outcomeIds = outcomeCohortIds,
    exposureTable = config$tables$cohort,
    outcomeTable = config$tables$outcomeCohort
  )

  results <- do.call(getSccStats, c(args, analysisOptions))
  results$source_id <- config$sourceId
  results$analysis_id <- analysisId
  colnames(results)[colnames(results) == "EXPOSURE_ID"] <- "TARGET_COHORT_ID"
  colnames(results)[colnames(results) == "OUTCOME_ID"] <- "OUTCOME_COHORT_ID"

  return(results)
}


getSccStats <- function(connection,
                        cdmDatabaseSchema,
                        exposureIds,
                        outcomeIds,
                        outcomeDatabaseSchema,
                        exposureDatabaseSchema,
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
                        followupPeriod = 0) {
  if (riskWindowEndExposed < riskWindowStartExposed && !addLengthOfExposureExposed)
    stop("Risk window end (exposed) should be on or after risk window start")
  if (riskWindowEndUnexposed < riskWindowStartUnexposed && !addLengthOfExposureUnexposed)
    stop("Risk window end (unexposed) should be on or after risk window start")

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

  ParallelLogger::logInfo("Retrieving stats from database")
  sql <- SqlRender::readSql(system.file("sql/sql_server", "averageTimeOnTreatment.sql", package = "rewardb"))
  DatabaseConnector::renderTranslateExecuteSql(
                                                   connection = connection,
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

  results <- DatabaseConnector::renderTranslateQuerySql(connection = connection, "SELECT * FROM #results;")
  DatabaseConnector::renderTranslateExecuteSql(connection = connection, "TRUNCATE TABLE #results; DROP TABLE #results;")

  return(results)
}
