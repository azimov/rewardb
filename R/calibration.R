getOutcomeControls <- function(appContext, targetCohortIds) {

  sql <- "
    SELECT r.*, o.type_id as outcome_type
    FROM @schema.result r
    INNER JOIN @schema.negative_control nc ON (
      r.target_cohort_id = nc.target_cohort_id AND nc.outcome_cohort_id = r.outcome_cohort_id
    )
    INNER JOIN @schema.outcome o ON r.outcome_cohort_id = o.outcome_cohort_id
    WHERE r.TARGET_COHORT_ID IN (@target_cohort_ids)
    AND r.calibrated = 0 -- Using calibrated results makes no sense but flag just in case
    AND o.type_id != 2 -- ATLAS cohorts excluded
  "
  dbConn <- DatabaseConnector::connect(connectionDetails = appContext$connectionDetails)
  negatives <- DatabaseConnector::renderTranslateQuerySql(dbConn, sql, target_cohort_ids = targetCohortIds,
                                                          schema=appContext$short_name)
  DatabaseConnector::disconnect(dbConn)

  return(negatives)
}

getUncalibratedOutcomes <- function(appContext, targetCohortIds) {
  sql <- "
      SELECT r.*, o.type_id as outcome_type FROM @schema.result r
      LEFT JOIN @schema.negative_control nc ON (
        r.target_cohort_id = nc.target_cohort_id AND nc.outcome_cohort_id = r.outcome_cohort_id
      )
      INNER JOIN @schema.outcome o ON r.outcome_cohort_id = o.outcome_cohort_id
      WHERE r.TARGET_COHORT_ID IN (@target_cohort_ids)
      AND r.calibrated = 0
      AND nc.target_cohort_id IS NULL -- We only want entries that are not negative controls
      AND o.type_id != 2 -- ATLAS cohorts excluded
  "
  dbConn <- DatabaseConnector::connect(connectionDetails = appContext$connectionDetails)
  positives <- DatabaseConnector::renderTranslateQuerySql(dbConn, sql, target_cohort_ids = targetCohortIds,
                                                          schema=appContext$short_name)
  DatabaseConnector::disconnect(dbConn)

  return(positives)
}

getUncalibratedAtlasCohorts <- function(appContext, targetCohortIds) {
  sql <- "
      SELECT r.*, o.type_id as outcome_type
      FROM @schema.result r
      INNER JOIN @schema.outcome o ON r.outcome_cohort_id = o.outcome_cohort_id
      WHERE r.TARGET_COHORT_ID IN (@target_cohort_ids)
      AND r.calibrated = 0
      AND o.type_id = 2 -- ATLAS cohorts only
  "
  dbConn <- DatabaseConnector::connect(connectionDetails = appContext$connectionDetails)
  positives <- DatabaseConnector::renderTranslateQuerySql(dbConn, sql, target_cohort_ids = targetCohortIds,
                                                          schema=appContext$short_name)
  DatabaseConnector::disconnect(dbConn)

  return(positives)
}


#'@export
calibrateTargets <- function(appContext, targetCohortIds) {
  # get negative control data rows
  controlOutcomes <- getOutcomeControls(appContext, targetCohortIds)
  # get positives
  positives <- getUncalibratedOutcomes(appContext, targetCohortIds)

  resultSet <- data.frame()
  dataSources = unique(positives$SOURCE_ID)
  outcomeTypes = unique(positives$OUTCOME_TYPE)

  print(paste("calibrating", nrow(positives) ,"outcomes"))
  for (outcomeType in outcomeTypes) {
    for (targetId in targetCohortIds) {
      for (dataSource in dataSources) {

        negatives <- controlOutcomes[
          controlOutcomes$OUTCOME_TYPE == outcomeType &
            controlOutcomes$SOURCE_ID == dataSource &
            controlOutcomes$TARGET_COHORT_ID == targetId,]

        interest <- positives[
            positives$OUTCOME_TYPE == outcomeType &
            positives$TARGET_COHORT_ID == targetId &
            positives$SOURCE_ID == dataSource,]

        if (nrow(negatives) > 0) {
          nullDist <- EmpiricalCalibration::fitNull(logRr = log(negatives$RR), seLogRr = negatives$SE_LOG_RR)
          calibratedPValue <- EmpiricalCalibration::calibrateP(nullDist, log(interest$RR), interest$SE_LOG_RR)
          errorModel <- EmpiricalCalibration::convertNullToErrorModel(nullDist)
          ci <- EmpiricalCalibration::calibrateConfidenceInterval(log(interest$RR), interest$SE_LOG_RR, errorModel)

          result <- data.frame(
            SOURCE_ID = dataSource,
            TARGET_COHORT_ID = targetId,
            CALIBRATED = 1,
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

          resultSet <- rbind(resultSet, result)
        } else {
          print(paste("No negative control outcomes for ", targetId, "were found on source", dataSource))
        }
      }
    }
  }

  dbConn <- DatabaseConnector::connect(connectionDetails = appContext$connectionDetails)
  DatabaseConnector::dbAppendTable(dbConn, paste0(appContext$short_name, ".result"), resultSet)
  DatabaseConnector::disconnect(dbConn)
}

#'@export
calibrateCustomCohorts <- function(appContext, targetCohortIds) {
  # get negative control data rows
  controlOutcomes <- getOutcomeControls(appContext, targetCohortIds)
  positives <- getUncalibratedAtlasCohorts(appContext, targetCohortIds)

  resultSet <- data.frame()
  dataSources = unique(positives$SOURCE_ID)
  for(dataSource in dataSources) {
      negatives <- controlOutcomes[ controlOutcomes$SOURCE_ID == dataSource,]
      interest <- positives[positives$SOURCE_ID == dataSource,]

      nullDist <- EmpiricalCalibration::fitNull(logRr = log(negatives$RR), seLogRr = negatives$SE_LOG_RR)
          calibratedPValue <- EmpiricalCalibration::calibrateP(nullDist, log(interest$RR), interest$SE_LOG_RR)
          errorModel <- EmpiricalCalibration::convertNullToErrorModel(nullDist)
          ci <- EmpiricalCalibration::calibrateConfidenceInterval(log(interest$RR), interest$SE_LOG_RR, errorModel)
      if (nrow(negatives) > 0) {
          result <- data.frame(
            SOURCE_ID = dataSource,
            TARGET_COHORT_ID = interest$TARGET_COHORT_ID,
            CALIBRATED = 1,
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
          resultSet <- rbind(resultSet, result)
      } else {
        print("Error no negative controls mapped")
      }
  }

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

#' Input should specify a csv file with the columns target_concept_id and outcome_cohort_id
#' This script will then work out the target cohorts for the specified outcome cohort id
addCustomCohortNegativeControls <- function (appContext, csvFilePath) {
}