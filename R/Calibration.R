#' @title
#' Get outcome controls
#' @description
#' Returns a set of specified negative controls for input target cohorts
#' @param appContext rewardb app context
#' @param connection DatabaseConnector connection to postgres rewardb instance
#' @param targetIds cohorts to get negative controls for
#' @param minCohortSize smaller cohorts are not generally used for calibration as rr values tend to be extremes
#' @return data.frame of negative control exposures for specified outcomes
getOutcomeControls <- function(appContext, connection, targetIds = NULL, outcomeTypes = c(0,1), minCohortSize = 10) {

  sql <- "
    SELECT r.*, o.type_id as outcome_type
    FROM @schema.result r
    INNER JOIN @schema.negative_control nc ON (
      r.target_cohort_id = nc.target_cohort_id AND nc.outcome_cohort_id = r.outcome_cohort_id
    )
    INNER JOIN @schema.outcome o ON r.outcome_cohort_id = o.outcome_cohort_id
    AND r.calibrated = 0
    AND T_CASES >= @min_cohort_size
    AND o.type_id IN (@outcome_types)
    {@target_cohort_ids != ''} ? {AND r.target_cohort_id IN (@target_cohort_ids)}
  "
  negatives <- DatabaseConnector::renderTranslateQuerySql(
    connection,
    sql,
    schema = appContext$short_name,
    min_cohort_size = minCohortSize,
    outcome_types = outcomeTypes,
    target_cohort_ids = targetIds
  )
  return(negatives)
}

#' @title
#' Get exposure controls
#' @description
#' Returns a set of specified negative controls for input outcome cohorts
#' @param appContext rewardb app context
#' @param connection DatabaseConnector connection to postgres rewardb instance
#' @param minCohortSize smaller cohorts are not generally used for calibration as rr values tend to be extremes
#' @return data.frame of negative control exposures for specified outcomes
getExposureControls <- function(appContext, connection, minCohortSize = 10) {

  sql <- "
    SELECT r.*, o.type_id as outcome_type
    FROM @schema.result r
    INNER JOIN @schema.negative_control nc ON (
      r.target_cohort_id = nc.target_cohort_id AND nc.outcome_cohort_id = r.outcome_cohort_id
    )
    INNER JOIN @schema.outcome o ON r.outcome_cohort_id = o.outcome_cohort_id
    AND r.calibrated = 0
    AND T_CASES >= @min_cohort_size
  "
  negatives <- DatabaseConnector::renderTranslateQuerySql(
    connection,
    sql,
    schema = appContext$short_name,
    min_cohort_size = minCohortSize
  )

  return(negatives)
}

#' @title
#' Get uncalibrated outcomes
#' @description
#' get outcomes that are not calibrated
#' @param appContext rewardb app context
#' @return data.frame of uncalibrated results from databases
getUncalibratedOutcomes <- function(appContext) {
  sql <- "
      SELECT r.*, o.type_id as outcome_type FROM @schema.result r
      INNER JOIN @schema.outcome o ON r.outcome_cohort_id = o.outcome_cohort_id
      AND r.calibrated = 0
      AND o.type_id != 2 -- ATLAS cohorts excluded
  "
  dbConn <- DatabaseConnector::connect(connectionDetails = appContext$connectionDetails)
  on.exit(DatabaseConnector::disconnect(dbConn))
  positives <- DatabaseConnector::renderTranslateQuerySql(dbConn, sql, schema = appContext$short_name)
  return(positives)
}

#' @title
#' Get uncalibrated exposures
#' @description
#' get exposures that are not calibrated
#' @param appContext rewardb app context
#' @return data.frame of uncalibrated results from databases
getUncalibratedExposures <- function(appContext) {
  sql <- "
      SELECT r.*, o.type_id as outcome_type FROM @schema.result r
      INNER JOIN @schema.outcome o ON r.outcome_cohort_id = o.outcome_cohort_id
      AND r.calibrated = 0
      AND o.type_id != 2 -- ATLAS cohorts excluded
  "
  dbConn <- DatabaseConnector::connect(connectionDetails = appContext$connectionDetails)
  on.exit(DatabaseConnector::disconnect(dbConn))
  positives <- DatabaseConnector::renderTranslateQuerySql(dbConn, sql, schema = appContext$short_name)

  return(positives)
}

#' @title
#' Get uncalibrated Atlas cohorts
#' @description
#' Get atlas cohorts results pre-calibration
#' @param appContext rewardb app context
#' @return data.frame of uncalibrated results from databases
getUncalibratedAtlasCohorts <- function(appContext) {
  sql <- "
      SELECT r.*, o.type_id as outcome_type
      FROM @schema.result r
      INNER JOIN @schema.outcome o ON r.outcome_cohort_id = o.outcome_cohort_id
      AND r.calibrated = 0
      AND o.type_id = 2 -- ATLAS cohorts only
  "
  dbConn <- DatabaseConnector::connect(connectionDetails = appContext$connectionDetails)
  on.exit(DatabaseConnector::disconnect(dbConn))
  positives <- DatabaseConnector::renderTranslateQuerySql(dbConn, sql, schema = appContext$short_name)
  return(positives)
}

#' @title
#' Compute calibrated rows
#' @description
#' Actual calibration is performed here in a dplyr friendly way
#' @param positives this is the cohort set that should be calibrated
#' @param negatives these are the negative control cohort results
#' @param idCol - either target_cohort_id or outcome_cohort_id, this function is used in p
#' @param calibrationType - value stored in calibrated column of table
#' @return data.frame
computeCalibratedRows <- function(positives, negatives, idCol, calibrationType = 1) {
  nullDist <- EmpiricalCalibration::fitNull(logRr = log(negatives$RR), seLogRr = negatives$SE_LOG_RR)
  calibratedPValue <- EmpiricalCalibration::calibrateP(nullDist, log(positives$RR), positives$SE_LOG_RR)
  errorModel <- EmpiricalCalibration::convertNullToErrorModel(nullDist)
  ci <- EmpiricalCalibration::calibrateConfidenceInterval(log(positives$RR), positives$SE_LOG_RR, errorModel)

  # Row matches fields in the database excluding the ids, used in dplyr, group_by with keep_true
  result <- data.frame(
    CALIBRATED = calibrationType,
    P_VALUE = calibratedPValue,
    UB_95 = exp(ci$logUb95Rr),
    LB_95 = exp(ci$logLb95Rr),
    RR = exp(ci$logRr),
    SE_LOG_RR = ci$seLogRr,
    C_CASES = positives$C_CASES,
    C_PT = positives$C_PT,
    C_AT_RISK = positives$C_AT_RISK,
    T_CASES = positives$T_CASES,
    T_PT = positives$T_PT,
    T_AT_RISK = positives$T_AT_RISK,
    STUDY_DESIGN = positives$STUDY_DESIGN
  )

  result[, idCol] <- positives[, idCol]
  return(result)
}

.removeCalibratedResults <- function(appContext, connection) {
  DatabaseConnector::renderTranslateExecuteSql(
    connection,
    "DELETE FROM @schema.result r WHERE r.calibrated = 1",
    schema = appContext$short_name
  )
}


#' @title
#' Get calibrated Atlas cohorts
#' @description
#' Compute calibtation on atlas targets
#' @param appContext rewardb app context
#' @return data.frame of uncalibrated results
getCalibratedAtlasTargets <- function(appContext, connection) {
  library(dplyr)
  # Apply to atlas cohorts
  controlOutcomes <- getOutcomeControls(appContext, connection)
  atlasPositives <- getUncalibratedAtlasCohorts(appContext)
  message(paste("Calibrating", nrow(atlasPositives), "atlas outcomes"))
  resultSetAtlas <- atlasPositives %>%
    group_by(OUTCOME_TYPE, SOURCE_ID, TARGET_COHORT_ID) %>%
    group_modify(~computeCalibratedRows(.x, controlOutcomes[
      controlOutcomes$OUTCOME_TYPE == 0 &
        controlOutcomes$TARGET_COHORT_ID == .x$TARGET_COHORT_ID[1] &
        controlOutcomes$SOURCE_ID == .x$SOURCE_ID[1],
    ], idCol = "OUTCOME_COHORT_ID"), .keep = TRUE)

  resultSetAtlas <- data.frame(resultSetAtlas[, !(names(resultSetAtlas) %in% c("OUTCOME_TYPE"))])
  message(paste("Computed", nrow(resultSetAtlas), "calibrations"))

  return(resultSetAtlas)
}

#' @title
#' Get calibrated generic, not atlas cohorts
#' @description
#' Compute calibtation on atlas targets
#' @param appContext rewardb app context
#' @return data.frame of calibrated results
getCalibratedGenericTargets <- function(appContext, connection) {
   # get negative control data rows
  controlOutcomes <- getOutcomeControls(appContext, connection)
  # get positives
  positives <- getUncalibratedOutcomes(appContext)
  message(paste("calibrating", nrow(positives), "outcomes"))
  library(dplyr)

  # Get all outcomes for a given target, source and outcome type
  resultSet <- positives %>%
    group_by(OUTCOME_TYPE, SOURCE_ID, TARGET_COHORT_ID) %>%
    group_modify(~computeCalibratedRows(.x, controlOutcomes[
      controlOutcomes$OUTCOME_TYPE == .x$OUTCOME_TYPE[1] &
        controlOutcomes$TARGET_COHORT_ID == .x$TARGET_COHORT_ID[1] &
        controlOutcomes$SOURCE_ID == .x$SOURCE_ID[1],
    ], idCol = "OUTCOME_COHORT_ID"), .keep = TRUE)

  resultSet <- data.frame(resultSet[, !(names(resultSet) %in% c("OUTCOME_TYPE"))])
  message(paste("Computed", nrow(resultSet), "calibrations"))
  return(resultSet)
}

#' @title
#' Get all calibrated target cohorts
#' @description
#' Compute the calibrated results for cohort targets
#' Requires negative control cohorts to be set
#' @param appContext takes a rewardb application context
#' @return data.frame of calibrated results
getCalibratedTargets <- function(appContext, connection) {
  return(rbind(getCalibratedAtlasTargets(appContext, connection), getCalibratedGenericTargets(appContext, connection)))
}

#' @title
#' Get all calibrated outcome cohorts
#' @description
#' Compute the calibrated results for cohort outcomes
#' Requires negative control cohorts to be set
#' Used automatically when appContext$useExposureControls is TRUE
#' @param appContext takes a rewardb application context
#' @return data.frame of calibrated results
getCalibratedOutcomes <- function(appContext, connection) {
  # get negative control data rows
  controlExposures <- getExposureControls(appContext, connection)
  # get positives
  positives <- getUncalibratedExposures(appContext)
  ParallelLogger::logDebug(paste("calibrating", nrow(positives), "exposures"))
  library(dplyr)

  # Get all exposures for a given outcome, source and outcome type
  resultSet <- positives %>%
    group_by(OUTCOME_TYPE, SOURCE_ID, OUTCOME_COHORT_ID) %>%
    group_modify(~computeCalibratedRows(.x, controlExposures[
      controlExposures$OUTCOME_TYPE == .x$OUTCOME_TYPE[1] &
        controlExposures$OUTCOME_COHORT_ID == .x$OUTCOME_COHORT_ID[1] &
        controlExposures$SOURCE_ID == .x$SOURCE_ID[1],
    ], idCol = "TARGET_COHORT_ID"), .keep = TRUE)
  resultSet <- data.frame(resultSet[, !(names(resultSet) %in% c("OUTCOME_TYPE"))])

  # get negative control data rows -- type 0 outcomes only
  positives <- getUncalibratedAtlasCohorts(appContext)

  ParallelLogger::logDebug(paste("calibrating", nrow(positives), "exposures"))
  # Get all outcomes for a given target, source for outcome type 0
  # Compute calibrated results
  atlasResultSet <- positives %>%
    group_by(SOURCE_ID, OUTCOME_COHORT_ID) %>%
    group_modify(~computeCalibratedRows(.x, controlExposures[
      controlExposures$OUTCOME_COHORT_ID == .x$OUTCOME_COHORT_ID[1] &
        controlExposures$SOURCE_ID == .x$SOURCE_ID[1],
    ], idCol = "TARGET_COHORT_ID"), .keep = TRUE)

  return(rbind(resultSet, atlasResultSet))
}