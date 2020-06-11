dbControlCounts <- function (appContext, controlOutcomes) {
  
  targetIds <- appContext$target_concept_ids * 1000
  
  rcount <- data.frame()
  for (targetId in targetIds) {
    
    cl = controlOutcomes$OUTCOME_COHORT_ID %% 100 == 0
    cl2 = controlOutcomes$OUTCOME_COHORT_ID %% 100 == 1
    rcount <- rbind(rcount,
        data.frame(
          target_id = targetId/1000,
          Optum_0 = nrow(controlOutcomes[cl & controlOutcomes$SOURCE_ID == 10 & controlOutcomes$TARGET_COHORT_ID == targetId,]),
          CCAE_0 = nrow(controlOutcomes[cl & controlOutcomes$SOURCE_ID == 11 & controlOutcomes$TARGET_COHORT_ID == targetId,]),
          MDCD_0 = nrow(controlOutcomes[cl & controlOutcomes$SOURCE_ID == 12 & controlOutcomes$TARGET_COHORT_ID == targetId,]),
          MDCR_0 = nrow(controlOutcomes[cl &controlOutcomes$SOURCE_ID == 13 & controlOutcomes$TARGET_COHORT_ID == targetId,]),
          Optum_1 = nrow(controlOutcomes[cl2 & controlOutcomes$SOURCE_ID == 10 & controlOutcomes$TARGET_COHORT_ID == targetId,]),
          CCAE_1 = nrow(controlOutcomes[cl2 & controlOutcomes$SOURCE_ID == 11 & controlOutcomes$TARGET_COHORT_ID == targetId,]),
          MDCD_1 = nrow(controlOutcomes[cl2 & controlOutcomes$SOURCE_ID == 12 & controlOutcomes$TARGET_COHORT_ID == targetId,]),
          MDCR_1 = nrow(controlOutcomes[cl2 &controlOutcomes$SOURCE_ID == 13 & controlOutcomes$TARGET_COHORT_ID == targetId,])
        )
    )
  }
  
  return(rcount)
}

manualNegativeControlCalculation <- function(appContext, pdwConnection) {
  
  if (is.null(appContext$negative_control_outcome_list)) {
    stop("no negative_control_outcome_list option specified in configuration")
  }
  
  controlList <- read.csv(appContext$negative_control_outcome_list)
  
  controlOutcomeIds <- append(controlList$concept_id * 100, controlList$concept_id * 100 + 1)
  targetIds <- appContext$target_concept_ids * 1000
  
  sql <- "SELECT * FROM @results_schema.@results_table 
    WHERE OUTCOME_COHORT_ID IN (@outcomeIds) AND TARGET_COHORT_ID IN (@targetIds) AND LOG_RR IS NOT NULL"
  controlOutcomes <- DatabaseConnector::renderTranslateQuerySql(connection = pdwConnection, sql = sql, results_schema = appContext$resultsDatabase$schema,
                                                                results_table = appContext$resultsDatabase$asurvResultsTable,
                                                                outcomeIds = controlOutcomeIds, 
                                                                targetIds = targetIds)
  
  sql <- "SELECT * FROM @results_schema.@results_table
  WHERE OUTCOME_COHORT_ID NOT IN (@outcomeIds) AND TARGET_COHORT_ID IN (@targetIds) AND LOG_RR IS NOT NULL"
  
  interestingOutcomes <- DatabaseConnector::renderTranslateQuerySql(connection = pdwConnection, sql = sql, 
                                                                    results_schema = appContext$resultsDatabase$schema,
                                                                    results_table = appContext$resultsDatabase$asurvResultsTable,
                                                                    outcomeIds = controlOutcomeIds, 
                                                                    targetIds = targetIds)
  
  dataSources <- unique(interestingOutcomes$SOURCE_ID)
  
  print(paste("Computing negative controls", nrow(controlOutcomes), "for outcomes", nrow(interestingOutcomes)))
  
  resultSet <- data.frame()
  targetControlOutcomes <- data.frame()
  
  for(outcomeType in c(0, 1)){
    for(targetId in targetIds) {
      for(dataSource in dataSources){
        
        outcomes <- controlList$concept_id * 100 + outcomeType
        negatives <- controlOutcomes[
          controlOutcomes$OUTCOME_COHORT_ID %in% outcomes & 
            controlOutcomes$SOURCE_ID == dataSource &
              controlOutcomes$TARGET_COHORT_ID == targetId, ]
        
        interest <- interestingOutcomes[
          interestingOutcomes$OUTCOME_COHORT_ID %% 100 == outcomeType &
            interestingOutcomes$TARGET_COHORT_ID == targetId & interestingOutcomes$SOURCE_ID == dataSource, ]
        
        tcoRow <- data.frame(
          target_id = targetId,
          source_id = dataSource,
          outcome_type = outcomeType,
          controls_used = nrow(negatives),
          null_dist_mean = NA,
          null_dist_sd = NA
        )
        if (nrow(negatives) > 0) {
          
          nullDist <- EmpiricalCalibration::fitNull(logRr = negatives$LOG_RR, seLogRr = negatives$SE_LOG_RR)
          calibratedPValue <- EmpiricalCalibration::calibrateP(nullDist, interest$LOG_RR, interest$SE_LOG_RR)
          errorModel <- EmpiricalCalibration::convertNullToErrorModel(nullDist)
          ci <- EmpiricalCalibration::calibrateConfidenceInterval(interest$LOG_RR, interest$SE_LOG_RR, errorModel)
          
          result <- data.frame(
            source_id = dataSource,
            target_cohort_id = targetId,
            outcome_cohort_id = interest$OUTCOME_COHORT_ID,
            uncalibrated_p_value=interest$P_VALUE,
            uncalibrated_rr=exp(interest$LOG_RR),
            uncalibrated_ub_95 = exp(interest$LB_95),
            uncalibrated_lb_95 = exp(interest$UB_95),
            uncalibrated_seLogRr = interest$SE_LOG_RR, 
            calibrated_p_value = calibratedPValue,
            calibrated_ub_95 = exp(ci$logUb95Rr),
            calibrated_lb_95 = exp(ci$logLb95Rr),
            calibrated_rr = exp(ci$logRr),
            calibrated_seLogRr = ci$seLogRr
          )
          tcoRow$null_dist_mean <- nullDist[1]
          tcoRow$null_dist_sd <- nullDist[2]
          
          resultSet <- rbind(resultSet, result)
        }
        targetControlOutcomes <- rbind(targetControlOutcomes, tcoRow)
      }
    }
  }
  # TODO: store results in db
  return (list(results=resultSet, controlCounts=targetControlOutcomes, dbControlCounts=dbControlCounts(appContext, controlOutcomes)))
}

makeControlPlot <- function(appContext, pdwConnection, minExposure=30, outfile=NULL) {
  
  if (is.null(appContext$negative_control_outcome_list)) {
    stop("no negative_control_outcome_list option specified in configuration")
  }
  
  controlList <- read.csv(appContext$negative_control_outcome_list)
  
  controlOutcomeIds <- append(controlList$concept_id * 100, controlList$concept_id * 100 + 1)
  targetIds <- appContext$target_concept_ids * 1000
  
  sql <- "SELECT * FROM @results_schema.@results_table 
  WHERE OUTCOME_COHORT_ID IN (@outcomeIds) AND TARGET_COHORT_ID IN (@targetIds) AND LOG_RR IS NOT NULL
  AND T_CASES >= @min_exposure
  "
  controlOutcomes <- DatabaseConnector::renderTranslateQuerySql(connection = pdwConnection, sql = sql, results_schema = appContext$resultsDatabase$schema,
                                                                results_table = appContext$resultsDatabase$asurvResultsTable,
                                                                outcomeIds = controlOutcomeIds, 
                                                                targetIds = targetIds,
                                                                min_exposure = minExposure
                                                        )
  
  print(paste("Plotting #", nrow(controlOutcomes)))
  return(EmpiricalCalibration::plotCalibrationEffect(controlOutcomes$LOG_RR, controlOutcomes$SE_LOG_RR));
}

moreStats <- function(results) {
  data.frame(
    signficantUncalibrated = nrow(results[results$uncalibrated_rr < 0.5 & results$uncalibrated_p_value < 0.05,]),
    significantCalibrated = nrow(results[results$calibrated_rr < 0.5 & results$calibrated_p_value < 0.05,]),
    reclassifiedInsignificant = nrow(results[results$uncalibrated_rr < 0.5 & results$uncalibrated_p_value < 0.05 & ( results$calibrated_p_value > 0.05 | results$calibrated_rr >= 0.5), ]),
    reclassifiedSignificant = nrow(results[results$uncalibrated_rr >= 0.5 & results$calibrated_p_value < 0.05 & results$calibrated_rr < 0.5, ])
  )
}

appContext <- loadAppContext("config/config.tnfs.yml", TRUE, TRUE)
res <- manualNegativeControlCalculation(appContext, appContext$cdmConnection)

appContext$negative_control_outcome_list <- "extra/negative_controls/anti_tnfi_full_atlas.csv"
resAtlasFull <- manualNegativeControlCalculation(appContext, appContext$cdmConnection)
