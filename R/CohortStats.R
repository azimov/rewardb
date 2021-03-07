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
getAverageTimeOnTreatment <- function(config, analysisOptions = list(), analysisId = NULL, targetCohortIds = NULL, outcomeCohortIds = NULL) {
  args <- list(
    connectionDetails = config$connectionDetails,
    cdmDatabaseSchema = config$cdmSchema,
    outcomeDatabaseSchema = config$resultSchema,
    exposureDatabaseSchema = config$resultSchema,
    exposureIds = targetCohortIds,
    outcomeIds = outcomeCohortIds,
    exposureTable = config$tables$cohort,
    outcomeTable = config$tables$outcomeCohort
  )

  results <- do.call(getSCCExposureStats, c(args, analysisOptions))

  if (nrow(results) > 0) {
    results$source_id <- config$sourceId
    results$analysis_id <- as.integer(analysisId)
    colnames(results)[colnames(results) == "EXPOSURE_ID"] <- "TARGET_COHORT_ID"
    colnames(results)[colnames(results) == "OUTCOME_ID"] <- "OUTCOME_COHORT_ID"
  }

  return(results)
}

#' @title
#' Get Self-Controlled cohort stats
#' @description
#' Generate time to outcome and time on treatment statistics.
#' Takes the same parameters as the self-controlled cohort call
#'
#' @param connectionDetails                An R object of type \code{connectionDetails} created using
#'                                         the function \code{createConnectionDetails} in the
#'                                         \code{DatabaseConnector} package.
#' @param cdmDatabaseSchema                Name of database schema that contains the OMOP CDM and
#'                                         vocabulary.
#' @param cdmVersion                       Define the OMOP CDM version used: currently support "4" and
#'                                         "5".
#' @param oracleTempSchema                 For Oracle only: the name of the database schema where you
#'                                         want all temporary tables to be managed. Requires
#'                                         create/insert permissions to this database.
#' @param exposureIds                      A vector containing the drug_concept_ids or
#'                                         cohort_definition_ids of the exposures of interest. If empty,
#'                                         all exposures in the exposure table will be included.
#' @param outcomeIds                       The condition_concept_ids or cohort_definition_ids of the
#'                                         outcomes of interest. If empty, all the outcomes in the
#'                                         outcome table will be included.
#' @param exposureDatabaseSchema           The name of the database schema that is the location where
#'                                         the exposure data used to define the exposure cohorts is
#'                                         available. If exposureTable = DRUG_ERA,
#'                                         exposureDatabaseSchema is not used by assumed to be
#'                                         cdmSchema.  Requires read permissions to this database.
#' @param exposureTable                    The tablename that contains the exposure cohorts.  If
#'                                         exposureTable <> DRUG_ERA, then expectation is exposureTable
#'                                         has format of COHORT table: cohort_concept_id, SUBJECT_ID,
#'                                         COHORT_START_DATE, COHORT_END_DATE.
#' @param outcomeDatabaseSchema            The name of the database schema that is the location where
#'                                         the data used to define the outcome cohorts is available. If
#'                                         exposureTable = CONDITION_ERA, exposureDatabaseSchema is not
#'                                         used by assumed to be cdmSchema.  Requires read permissions
#'                                         to this database.
#' @param outcomeTable                     The tablename that contains the outcome cohorts.  If
#'                                         outcomeTable <> CONDITION_OCCURRENCE, then expectation is
#'                                         outcomeTable has format of COHORT table:
#'                                         COHORT_DEFINITION_ID, SUBJECT_ID, COHORT_START_DATE,
#'                                         COHORT_END_DATE.
#' @param firstExposureOnly                If TRUE, only use first occurrence of each drug concept idgetSccStats
#'                                         for each person
#' @param firstOutcomeOnly                 If TRUE, only use first occurrence of each condition concept
#'                                         id for each person.
#' @param minAge                           Integer for minimum allowable age.
#' @param maxAge                           Integer for maximum allowable age.
#' @param studyStartDate                   Date for minimum allowable data for index exposure. Date
#'                                         format is 'yyyymmdd'.
#' @param studyEndDate                     Date for maximum allowable data for index exposure. Date
#'                                         format is 'yyyymmdd'.
#' @param addLengthOfExposureExposed       If TRUE, use the duration from drugEraStart -> drugEraEnd as
#'                                         part of timeAtRisk.
#' @param riskWindowStartExposed           Integer of days to add to drugEraStart for start of
#'                                         timeAtRisk (0 to include index date, 1 to start the day
#'                                         after).
#' @param riskWindowEndExposed             Additional window to add to end of exposure period (if
#'                                         addLengthOfExposureExposed = TRUE, then add to exposure end
#'                                         date, else add to exposure start date).
#' @param addLengthOfExposureUnexposed     If TRUE, use the duration from exposure start -> exposure
#'                                         end as part of timeAtRisk looking back before exposure
#'                                         start.
#' @param riskWindowEndUnexposed           Integer of days to add to exposure start for end of
#'                                         timeAtRisk (0 to include index date, -1 to end the day
#'                                         before).
#' @param riskWindowStartUnexposed         Additional window to add to start of exposure period (if
#'                                         addLengthOfExposureUnexposed = TRUE, then add to exposure
#'                                         end date, else add to exposure start date).
#' @param hasFullTimeAtRisk                If TRUE, restrict to people who have full time-at-risk
#'                                         exposed and unexposed.
#' @param washoutPeriod                    Integer to define required time observed before exposure
#'                                         start.
#' @param followupPeriod                   Integer to define required time observed after exposure
#'                                         start.
#' @param computeThreads                   Number of parallel threads for computing IRRs with exact
#'                                         confidence intervals.
#'
#' @return
#' A data frame  containing the results of the analysis.
#' @examples
#' \dontrun{
#' connectionDetails <- createConnectionDetails(dbms = "sql server",
#'                                              server = "RNDUSRDHIT07.jnj.com")
#' sccResult <- getSelfControlledCohortExposureStats(connectionDetails,
#'                                      cdmDatabaseSchema = "cdm_truven_mdcr.dbo",
#'                                      exposureIds = c(767410, 1314924, 907879),
#'                                      outcomeIds = 444382,
#'                                      outcomeTable = "condition_era")
#' }
#' @export
getSCCExposureStats <- function(connectionDetails,
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

  # Check if connection already open:
  if (is.null(connectionDetails$conn())) {
    conn <- DatabaseConnector::connect(connectionDetails)
  } else {
    conn <- connectionDetails$conn()
  }

  ParallelLogger::logInfo("Retrieving stats from database")

  if (length(outcomeIds)) {
    DatabaseConnector::insertTable(conn, "#scc_outcome_ids", data.frame(outcome_id = outcomeIds), tempTable = TRUE)
  }

  if (length(exposureIds)) {
    DatabaseConnector::insertTable(conn, "#scc_exposure_ids", data.frame(exposure_id = exposureIds), tempTable = TRUE)
  }

  sql <- SqlRender::readSql(system.file("sql/sql_server", "averageTimeOnTreatment.sql", package = "rewardb"))
  DatabaseConnector::renderTranslateExecuteSql(
                                                   connection = conn,
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

  results <- DatabaseConnector::renderTranslateQuerySql(connection = conn, "SELECT * FROM #results;")
  DatabaseConnector::renderTranslateExecuteSql(connection = conn, "TRUNCATE TABLE #results; DROP TABLE #results;")

  if (is.null(connectionDetails$conn())) {
    DatabaseConnector::disconnect(conn)
  }

  delta <- Sys.time() - start
  ParallelLogger::logInfo(paste("Computing SCC statistics took", signif(delta, 3), attr(delta, "units")))

  return(results)
}
