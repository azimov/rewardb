getOutcomeControls <- function(appContext, targetCohortIds, minCohortSize=10) {

  sql <- "
    SELECT r.*, o.type_id as outcome_type
    FROM @schema.result r
    INNER JOIN @schema.negative_control nc ON (
      r.target_cohort_id = nc.target_cohort_id AND nc.outcome_cohort_id = r.outcome_cohort_id
    )
    INNER JOIN @schema.outcome o ON r.outcome_cohort_id = o.outcome_cohort_id
    WHERE r.TARGET_COHORT_ID IN (@target_cohort_ids)
    AND r.calibrated = 0
    AND T_CASES >= @min_cohort_size
  "

  dbConn <- DatabaseConnector::connect(connectionDetails = appContext$connectionDetails)
  negatives <- DatabaseConnector::renderTranslateQuerySql(
    dbConn,
    sql,
    target_cohort_ids = targetCohortIds,
    schema=appContext$short_name,
    min_cohort_size=minCohortSize
  )
  DatabaseConnector::disconnect(dbConn)

  return(negatives)
}

getExposureControls <- function(appContext, outcomeCohortIds, minCohortSize=10) {

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

  dbConn <- DatabaseConnector::connect(connectionDetails = appContext$connectionDetails)
  negatives <- DatabaseConnector::renderTranslateQuerySql(
    dbConn,
    sql,
    outcome_cohort_ids = outcomeCohortIds,
    schema=appContext$short_name,
    min_cohort_size=minCohortSize
  )
  DatabaseConnector::disconnect(dbConn)

  return(negatives)
}

getUncalibratedOutcomes <- function(appContext, targetCohortIds) {
  sql <- "
      SELECT r.*, o.type_id as outcome_type FROM @schema.result r
      INNER JOIN @schema.outcome o ON r.outcome_cohort_id = o.outcome_cohort_id
      WHERE r.TARGET_COHORT_ID IN (@target_cohort_ids)
      AND r.calibrated = 0
      AND o.type_id != 2 -- ATLAS cohorts excluded
  "
  dbConn <- DatabaseConnector::connect(connectionDetails = appContext$connectionDetails)
  positives <- DatabaseConnector::renderTranslateQuerySql(dbConn, sql, target_cohort_ids = targetCohortIds,
                                                          schema=appContext$short_name)
  DatabaseConnector::disconnect(dbConn)

  return(positives)
}

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

computeCalibratedRows <- function (interest, negatives, calibrationType = 1) {
  nullDist <- EmpiricalCalibration::fitNull(logRr = log(negatives$RR), seLogRr = negatives$SE_LOG_RR)
  calibratedPValue <- EmpiricalCalibration::calibrateP(nullDist, log(interest$RR), interest$SE_LOG_RR)
  errorModel <- EmpiricalCalibration::convertNullToErrorModel(nullDist)
  ci <- EmpiricalCalibration::calibrateConfidenceInterval(log(interest$RR), interest$SE_LOG_RR, errorModel)

  # Row matches fields in the database excluding the ids, used in dplyr, group_by with keep_true
  result <- data.frame(
      CALIBRATED = calibrationType,
      OUTCOME_COHORT_ID = interest$OUTCOME_COHORT_ID,
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
  return(result)
}

#'@export
calibrateTargets <- function(appContext, targetCohortIds) {
  # get negative control data rows
  controlOutcomes <- getOutcomeControls(appContext, targetCohortIds)
  # get positives
  positives <- getUncalibratedOutcomes(appContext, targetCohortIds)
  print(paste("calibrating", nrow(positives) ,"outcomes"))
  library(dplyr)

  # Get all outcomes for a given target, source and outcome type
  resultSet <- positives %>%
    group_by(OUTCOME_TYPE, SOURCE_ID, TARGET_COHORT_ID)  %>%
    group_modify(~ computeCalibratedRows(.x, controlOutcomes[
      controlOutcomes$OUTCOME_TYPE == .x$OUTCOME_TYPE[1] &
      controlOutcomes$TARGET_COHORT_ID == .x$TARGET_COHORT_ID[1] &
      controlOutcomes$SOURCE_ID == .x$SOURCE_ID[1],
    ]), .keep=TRUE)
  resultSet <- data.frame(resultSet[,!(names(resultSet) %in% c("OUTCOME_TYPE")) ])
  dbConn <- DatabaseConnector::connect(connectionDetails = appContext$connectionDetails)
  DatabaseConnector::dbAppendTable(dbConn, paste0(appContext$short_name, ".result"), data.frame(resultSet))
  DatabaseConnector::disconnect(dbConn)
}

#'@export
calibrateOutcomes <- function(appContext) {

  outcomeIds <- append(appContext$outcome_concept_ids * 100, appContext$outcome_concept_ids * 100 + 1)
  # get negative control data rows
  controlExposures <- getExposureControls(appContext, outcomeIds)
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
    ]), .keep=TRUE)
  resultSet <- data.frame(resultSet[,!(names(resultSet) %in% c("OUTCOME_TYPE")) ])
  dbConn <- DatabaseConnector::connect(connectionDetails = appContext$connectionDetails)
  DatabaseConnector::dbAppendTable(dbConn, paste0(appContext$short_name, ".result"), data.frame(resultSet))
  DatabaseConnector::disconnect(dbConn)
}


#'@export
calibrateCustomCohorts <- function(appContext, targetCohortIds) {
  # get negative control data rows
  controlOutcomes <- getOutcomeControls(appContext, targetCohortIds)
  positives <- getUncalibratedAtlasCohorts(appContext)

  print(paste("calibrating", nrow(positives) ,"outcomes"))
  library(dplyr)

  # Get all outcomes for a given target, source for outcome type 0
  # Compute calibrated results
  resultSet <- positives %>%
    group_by(SOURCE_ID, TARGET_COHORT_ID)  %>%
    group_modify(~ computeCalibratedRows(.x, controlOutcomes[
      controlOutcomes$OUTCOME_TYPE == 0 &
      controlOutcomes$TARGET_COHORT_ID == .x$TARGET_COHORT_ID[1] &
      controlOutcomes$SOURCE_ID == .x$SOURCE_ID[1],
    ]), .keep=TRUE)

  resultSet <- data.frame(resultSet[,!(names(resultSet) %in% c("OUTCOME_TYPE")) ])
  dbConn <- DatabaseConnector::connect(connectionDetails = appContext$connectionDetails)
  DatabaseConnector::dbAppendTable(dbConn, paste0(appContext$short_name, ".result"), resultSet)
  DatabaseConnector::disconnect(dbConn)
}


#'@export
calibrateOutcomesCustomCohorts <- function(appContext) {
  # get negative control data rows -- type 0 outcomes only
  controlExposures <- getExposureControls(appContext, appContext$custom_outcome_cohort_ids)
  positives <- getUncalibratedAtlasCohorts(appContext)

  print(paste("calibrating", nrow(positives) ,"outcomes"))
  library(dplyr)

  # Get all outcomes for a given target, source for outcome type 0
  # Compute calibrated results
  resultSet <- positives %>%
    group_by(SOURCE_ID, OUTCOME_COHORT_ID)  %>%
    group_modify(~ computeCalibratedRows(.x, controlExposures[
      controlExposures$OUTCOME_COHORT_ID == .x$OUTCOME_COHORT_ID[1] &
      controlExposures$SOURCE_ID == .x$SOURCE_ID[1],
    ]), .keep=TRUE)

  resultSet <- data.frame(resultSet[,!(names(resultSet) %in% c("OUTCOME_TYPE")) ])
  dbConn <- DatabaseConnector::connect(connectionDetails = appContext$connectionDetails)
  DatabaseConnector::dbAppendTable(dbConn, paste0(appContext$short_name, ".result"), resultSet)
  DatabaseConnector::disconnect(dbConn)
}

#' Input should specify a csv file with the columns target_concept_id and outcome_concept_id
#' This script will then work out which cohorts they should apply to
#'@export
addManualNegativeOutcomeControls <- function (appContext) {

  dbConn <- DatabaseConnector::connect(connectionDetails = appContext$connectionDetails)
  dataFrame <- read.csv(appContext$negative_control_outcome_list)

  sql <- "INSERT INTO @schema.negative_control (outcome_cohort_id, target_cohort_id)
      SELECT outcome_cohort_id, target_cohort_id FROM @schema.outcome_concept, @schema.target
        WHERE condition_concept_id IN (@control_outcomes_ids)"

  DatabaseConnector::renderTranslateExecuteSql(dbConn, sql,
                                               schema=appContext$short_name,
                                               control_outcomes_ids=dataFrame$concept_id)

  DatabaseConnector::disconnect(dbConn)
}