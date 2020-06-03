manualNegativeControlCalculation <- function(appContext, connection, pdwConnection) {
  
  if (is.null(appContext$negative_control_outcome_list)) {
    stop("no negative_control_outcome_list option specified in configuration")
  }
  
  controlList <- read.csv(appContext$negative_control_list)
  
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
  for(outcomeType in c(0, 1)){
    for(targetId in targetIds) {
      for(dataSource in dataSources){
        
        outcomes <- controlList$concept_id * 100 + outcomeType
        negatives <- controlOutcomes[
          controlOutcomes$OUTCOME_COHORT_ID %in% outcomes & 
            controlOutcomes$SOURCE_ID == dataSource &
              controlOutcomes$TARGET_COHORT_ID == targetId, ]
        
        print(paste(targetId, "ds", dataSource, "using", nrow(negatives), "control outcomes"))
        if (nrow(negatives) > 0) {
          interest <- interestingOutcomes[
            interestingOutcomes$OUTCOME_COHORT_ID %% 100.0 == outcomeType &
            interestingOutcomes$TARGET_COHORT_ID == targetId & interestingOutcomes$SOURCE_ID == dataSource, ]
          
          nullDist <- EmpiricalCalibration::fitNull(logRr = negatives$LOG_RR, seLogRr = negatives$SE_LOG_RR)
          calibratedPValue <- EmpiricalCalibration::calibrateP(nullDist, interest$LOG_RR, interest$SE_LOG_RR)
          errorModel <- EmpiricalCalibration::convertNullToErrorModel(nullDist)
          ci <- EmpiricalCalibration::calibrateConfidenceInterval(interest$LOG_RR, interest$SE_LOG_RR, errorModel)
          
          result <- data.frame(
            source_id = dataSource,
            target_cohort_id = targetId,
            outcome_cohort_id = interest$OUTCOME_COHORT_ID,
            calibrated_p_value = calibratedPValue,
            calibrated_ub_95 = exp(ci$logUb95Rr),
            calibrated_lb_95 = exp(ci$logLb95Rr),
            calibrated_rr = exp(ci$logRr),
            n_controls_used = nrow(negatives),
            # Save null distributions
            null_dist_mean = nullDist[1],
            null_dist_sd = nullDist[2]
          )
          resultSet <- rbind(resultSet, result)
        } 
      }
    }
  }
  return (resultSet)
}
