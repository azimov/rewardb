#' Returns a set of specified negative controls for input target cohorts
#' @param appContext rewardb app context
#' @param connection DatabaseConnector connection to postgres rewardb instance
#' @param outcomeCohortIds cohorts to get negative controls for
#' @param minCohortSize smaller cohorts are not generally used for calibration as rr values tend to be extremes
#' @return data.frame of negative control exposures for specified outcomes
getOutcomeControls <- function(appContext, connection, minCohortSize=10) {

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
    schema=appContext$short_name,
    min_cohort_size=minCohortSize
  )
  return(negatives)
}

#' Returns a set of specified negative controls for input outcome cohorts
#' @param appContext rewardb app context
#' @param connection DatabaseConnector connection to postgres rewardb instance
#' @param outcomeCohortIds cohorts to get negative controls for
#' @param minCohortSize smaller cohorts are not generally used for calibration as rr values tend to be extremes
#' @return data.frame of negative control exposures for specified outcomes
getExposureControls <- function(appContext, connection, outcomeCohortIds, minCohortSize=10) {

  sql <- "
    SELECT r.*, o.type_id as outcome_type
    FROM @schema.result r
    INNER JOIN @schema.negative_control nc ON (
      r.target_cohort_id = nc.target_cohort_id AND nc.outcome_cohort_id = r.outcome_cohort_id
    )
    INNER JOIN @schema.outcome o ON r.outcome_cohort_id = o.outcome_cohort_id
    WHERE r.OUTCOME_COHORT_ID IN (@outcome_cohort_ids)
    AND r.calibrated = 0
    AND T_CASES >= @min_cohort_size
  "
  negatives <- DatabaseConnector::renderTranslateQuerySql(
    connection,
    sql,
    outcome_cohort_ids = outcomeCohortIds,
    schema=appContext$short_name,
    min_cohort_size=minCohortSize
  )

  return(negatives)
}

#' get outcomes that are not calibrated
#' param appContext rewardb app context
#' @return data.frame of uncalibrated results from databases
getUncalibratedOutcomes <- function(appContext) {
  sql <- "
      SELECT r.*, o.type_id as outcome_type FROM @schema.result r
      INNER JOIN @schema.outcome o ON r.outcome_cohort_id = o.outcome_cohort_id
      AND r.calibrated = 0
      AND o.type_id != 2 -- ATLAS cohorts excluded
  "
  dbConn <- DatabaseConnector::connect(connectionDetails = appContext$connectionDetails)
  positives <- DatabaseConnector::renderTranslateQuerySql(dbConn, sql, schema=appContext$short_name)
  DatabaseConnector::disconnect(dbConn)
  return(positives)
}

#' get exposures that are not calibrated
#' @param appContext rewardb app context
#' @return data.frame of uncalibrated results from databases
getUncalibratedExposures <- function(appContext, outcomeCohortIds) {
  sql <- "
      SELECT r.*, o.type_id as outcome_type FROM @schema.result r
      INNER JOIN @schema.outcome o ON r.outcome_cohort_id = o.outcome_cohort_id
      WHERE r.OUTCOME_COHORT_ID IN (@outcome_cohort_ids)
      AND r.calibrated = 0
      AND o.type_id != 2 -- ATLAS cohorts excluded
  "
  dbConn <- DatabaseConnector::connect(connectionDetails = appContext$connectionDetails)
  positives <- DatabaseConnector::renderTranslateQuerySql(dbConn, sql, outcome_cohort_ids = outcomeCohortIds,
                                                          schema=appContext$short_name)
  DatabaseConnector::disconnect(dbConn)

  return(positives)
}

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
  positives <- DatabaseConnector::renderTranslateQuerySql(dbConn, sql, schema=appContext$short_name)
  DatabaseConnector::disconnect(dbConn)
  return(positives)
}

#' Actual calibration is performed here in a dplyr friendly way
#' @param interest this is the cohort set that should be calibrated
#' @param negatives these are the negative control cohort results
#' @param idCol - either target_cohort_id or outcome_cohort_id, this function is used in p
#' @return data.frame
computeCalibratedRows <- function (interest, negatives, idCol, calibrationType = 1) {
  nullDist <- EmpiricalCalibration::fitNull(logRr = log(negatives$RR), seLogRr = negatives$SE_LOG_RR)
  calibratedPValue <- EmpiricalCalibration::calibrateP(nullDist, log(interest$RR), interest$SE_LOG_RR)
  errorModel <- EmpiricalCalibration::convertNullToErrorModel(nullDist)
  ci <- EmpiricalCalibration::calibrateConfidenceInterval(log(interest$RR), interest$SE_LOG_RR, errorModel)

  # Row matches fields in the database excluding the ids, used in dplyr, group_by with keep_true
  result <- data.frame(
      CALIBRATED = calibrationType,
      P_VALUE = calibratedPValue,
      UB_95 = exp(ci$logUb95Rr),
      LB_95 = exp(ci$logLb95Rr),
      RR = exp(ci$logRr),
      SE_LOG_RR = ci$seLogRr,
      C_CASES = interest$C_CASES,
      C_PT = interest$C_PT,
      C_AT_RISK = interest$C_AT_RISK,
      T_CASES = interest$T_CASES,
      T_PT = interest$T_PT,
      T_AT_RISK = interest$T_AT_RISK,
      STUDY_DESIGN = interest$STUDY_DESIGN
  )

  result[,idCol] <- interest[,idCol]
  return(result)
}

.removeCalibratedResults <- function(appContext) {
  connection <- DatabaseConnector::connect(connectionDetails = appContext$connectionDetails)
  DatabaseConnector::renderTranslateExecuteSql(
    connection,
    "DELETE FROM @schema.result r WHERE r.calibrated = 1",
     schema = appContext$short_name
  )
  DatabaseConnector::disconnect(connection)
}

#' Compute the calibrated results for cohort targets
#' Requires negative control cohorts to be set
#' @param appContext takes a rewardb application context
#' @param targetCohortIds - these are the cohort ids in the db and (currently) must be specified manually. TODO: remove
#' @export
calibrateTargets <- function(appContext) {
  # get negative control data rows
  dbConn <- DatabaseConnector::connect(connectionDetails = appContext$connectionDetails)

  controlOutcomes <- getOutcomeControls(appContext, dbConn)
  # get positives
  positives <- getUncalibratedOutcomes(appContext)
  message(paste("calibrating", nrow(positives) ,"outcomes"))
  library(dplyr)

  # Get all outcomes for a given target, source and outcome type
  resultSet <- positives %>%
    group_by(OUTCOME_TYPE, SOURCE_ID, TARGET_COHORT_ID)  %>%
    group_modify(~computeCalibratedRows(.x, controlOutcomes[
      controlOutcomes$OUTCOME_TYPE == .x$OUTCOME_TYPE[1] &
      controlOutcomes$TARGET_COHORT_ID == .x$TARGET_COHORT_ID[1] &
      controlOutcomes$SOURCE_ID == .x$SOURCE_ID[1],
    ], idCol = "OUTCOME_COHORT_ID"), .keep=TRUE)

  resultSet <- data.frame(resultSet[,!(names(resultSet) %in% c("OUTCOME_TYPE")) ])
  message(paste("Computed", nrow(resultSet), "calibrations"))

  # Apply to atlas cohorts
  atlasPositives <- getUncalibratedAtlasCohorts(appContext)
  message(paste("Calibrating", nrow(atlasPositives), "atlas outcomes"))
  resultSetAtlas <- atlasPositives %>%
    group_by(OUTCOME_TYPE, SOURCE_ID, TARGET_COHORT_ID)  %>%
    group_modify(~ computeCalibratedRows(.x, controlOutcomes[
      controlOutcomes$OUTCOME_TYPE == 0 &
      controlOutcomes$TARGET_COHORT_ID == .x$TARGET_COHORT_ID[1] &
      controlOutcomes$SOURCE_ID == .x$SOURCE_ID[1],
    ], idCol = "OUTCOME_COHORT_ID"), .keep=TRUE)

  resultSetAtlas <- data.frame(resultSetAtlas[,!(names(resultSetAtlas) %in% c("OUTCOME_TYPE")) ])
  message(paste("Computed", nrow(resultSet), "calibrations"))

  DatabaseConnector::dbAppendTable(dbConn, paste0(appContext$short_name, ".result"), rbind(resultSet, resultSetAtlas))
  DatabaseConnector::disconnect(dbConn)
}

#' Compute the calibrated results for custom cohort outcomes
#' Requires negative control cohorts to be set
#' @param appContext takes a rewardb application context
#' @param targetCohortIds - these are the cohort ids in the db and (currently) must be specified manually. TODO: remove
#' @export
calibrateCustomCohorts <- function(appContext) {
  # get negative control data rows
  dbConn <- DatabaseConnector::connect(connectionDetails = appContext$connectionDetails)
  controlOutcomes <- getOutcomeControls(appContext, dbConn)

  DatabaseConnector::dbAppendTable(dbConn, paste0(appContext$short_name, ".result"), resultSet)
  DatabaseConnector::disconnect(dbConn)
}


#' Compute the calibrated results for cohort outcomes
#' Requires negative control cohorts to be set
#' @param appContext takes a rewardb application context
#' @export
calibrateOutcomes <- function(appContext) {
  dbConn <- DatabaseConnector::connect(connectionDetails = appContext$connectionDetails)
  outcomeIds <- append(appContext$outcome_concept_ids * 100, appContext$outcome_concept_ids * 100 + 1)
  # get negative control data rows
  controlExposures <- getExposureControls(appContext, dbConn, outcomeIds)
  # get positives
  positives <- getUncalibratedExposures(appContext, outcomeIds)
  print(paste("calibrating", nrow(positives) ,"exposures"))
  library(dplyr)

  # Get all exposures for a given outcome, source and outcome type
  resultSet <- positives %>%
    group_by(OUTCOME_TYPE, SOURCE_ID, OUTCOME_COHORT_ID)  %>%
    group_modify(~ computeCalibratedRows(.x, controlExposures[
      controlExposures$OUTCOME_TYPE == .x$OUTCOME_TYPE[1] &
      controlExposures$OUTCOME_COHORT_ID == .x$OUTCOME_COHORT_ID[1] &
      controlExposures$SOURCE_ID == .x$SOURCE_ID[1],
    ], idCol = "TARGET_COHORT_ID"), .keep=TRUE)
  resultSet <- data.frame(resultSet[,!(names(resultSet) %in% c("OUTCOME_TYPE")) ])

  DatabaseConnector::dbAppendTable(dbConn, paste0(appContext$short_name, ".result"), resultSet)
  DatabaseConnector::disconnect(dbConn)
}

#' Compute the calibrated results for custom cohort outcomes
#' Requires negative control cohorts to be set
#' @param appContext takes a rewardb application context
#' @export
calibrateOutcomesCustomCohorts <- function(appContext) {
  # get negative control data rows -- type 0 outcomes only
  dbConn <- DatabaseConnector::connect(connectionDetails = appContext$connectionDetails)
  controlExposures <- getExposureControls(appContext, dbConn, appContext$custom_outcome_cohort_ids)
  positives <- getUncalibratedAtlasCohorts(appContext)

  print(paste("calibrating", nrow(positives) ,"exposures"))
  library(dplyr)

  # Get all outcomes for a given target, source for outcome type 0
  # Compute calibrated results
  resultSet <- positives %>%
    group_by(SOURCE_ID, OUTCOME_COHORT_ID)  %>%
    group_modify(~ computeCalibratedRows(.x, controlExposures[
      controlExposures$OUTCOME_COHORT_ID == .x$OUTCOME_COHORT_ID[1] &
      controlExposures$SOURCE_ID == .x$SOURCE_ID[1],
    ], idCol = "TARGET_COHORT_ID"), .keep=TRUE)

  resultSet <- data.frame(resultSet[,!(names(resultSet) %in% c("OUTCOME_TYPE")) ])
  DatabaseConnector::dbAppendTable(dbConn, paste0(appContext$short_name, ".result"), resultSet)
  DatabaseConnector::disconnect(dbConn)
}