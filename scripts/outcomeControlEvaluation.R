devtools::load_all()
library(dplyr)
library(gt)
config <- loadGlobalConfig("config/global-cfg.yml")
connection <- DatabaseConnector::connect(config$connectionDetails)

dataSources <- DatabaseConnector::renderTranslateQuerySql(connection, "SELECT * FROM @schema.data_source", schema=config$rewardbResultsSchema, snakeCaseToCamelCase = TRUE)


getSetResults <- function(manualControlData, automatedControlsData, manualFilename, automatedFilename) {

  EmpiricalCalibration::plotCalibrationEffect(log(manualControlData$rr), manualControlData$seLogRr, fileName = manualFilename)
  EmpiricalCalibration::plotCalibrationEffect(log(automatedControlsData$rr), automatedControlsData$seLogRr, fileName = automatedFilename)

  manualNullDist <- EmpiricalCalibration::fitNull(logRr = log(manualControlData$rr), seLogRr = manualControlData$seLogRr)
  automatedNullDist <- EmpiricalCalibration::fitNull(logRr = log(automatedControlsData$rr), seLogRr = automatedControlsData$seLogRr)

  mu1 <- exp(manualNullDist["mean"])
  sd1 <- exp(manualNullDist["sd"])
  mu2 <- exp(automatedNullDist["mean"])
  sd2 <- exp(automatedNullDist["sd"])

  z <- (mu1 - mu2) / sqrt(sd1**2 + sd2**2)
  p <- 2 * pnorm(-abs(z))

  mErr <- EmpiricalCalibration::computeExpectedAbsoluteSystematicError(manualNullDist)
  aErr <- EmpiricalCalibration::computeExpectedAbsoluteSystematicError(automatedNullDist)

  data.frame(manualMean = mu1,
             manualSd = sd1,
             automatedMean = mu2,
             automatedSd = sd2,
             z = z,
             p = p,
             manualAbsErr = mErr,
             automatedAbsErr = aErr,
             absErrorDiff = abs(mErr - aErr))

}


# Map exposure to reward cohorts (cohort id)
data("ohdsiNegativeControls", package = 'MethodEvaluation')
ohdsiNegativeControls <- ohdsiNegativeControls[ohdsiNegativeControls$type != "Exposure control",]

exposureOutcomePairs <- data.frame(exposureConceptId = ohdsiNegativeControls$targetId, outcomeConceptId = ohdsiNegativeControls$outcomeId)
manualControlData <- getConcetptCohortData(connection, config, exposureOutcomePairs)
saveRDS(manualControlData, "extra/outcomeControlEvaluationManualControlsDt.rds")
manualControlData <- readRDS("extra/outcomeControlEvaluationManualControlsDt.rds")

automatedExposureOutcomePairs <- getOutcomeControlConcepts(connection, config, unique(exposureOutcomePairs$exposureConceptId))
automatedControlsData <- getConcetptCohortData(connection, config, automatedExposureOutcomePairs)
saveRDS(automatedControlsData, "extra/outcomeControlEvaluationAutomatedControlsDt.rds")
automatedControlsData <- readRDS("extra/outcomeControlEvaluationAutomatedControlsDt.rds")

results <- data.frame()
for (sourceId in c(10, 11, 12, 13)) {
  for (exposureId in unique(exposureOutcomePairs$exposureConceptId)) {

    row <- getSetResults(manualControlData[manualControlData$sourceId == sourceId & manualControlData$exposureConceptId == exposureId,],
                         automatedControlsData[automatedControlsData$sourceId == sourceId & automatedControlsData$exposureConceptId == exposureId,],
                         paste0("extra/eval_results/manual_plot_sid", sourceId, "-eid", exposureId, ".png"),
                         paste0("extra/eval_results/auto_plot_sid", sourceId, "-eid", exposureId, ".png"))

    row$exposureId <- exposureId
    row$sourceId <- sourceId

    results <- rbind(results, row)
  }
}
saveRDS(results, "extra/outcomeControlEvaluationTable.rds")

exposureNames <- DatabaseConnector::renderTranslateQuerySql(connection,
                                           "SELECT drug_conceptset_id as exposure_id, cohort_definition_name as exposure_name FROM @schema.cohort_definition WHERE drug_conceptset_id in (@exposure_ids)",
                                           schema = config$rewardbResultsSchema,
                                           exposure_ids = unique(exposureOutcomePairs$exposureConceptId),
                                           snakeCaseToCamelCase = TRUE)

outputTable <- results %>% inner_join(dataSources, by="sourceId") %>%
  inner_join(exposureNames, by="exposureId")  %>%
  select(sourceName, exposureName, manualMean, manualSd, automatedMean, automatedSd, manualAbsErr, automatedAbsErr, absErrorDiff, z, p) %>% 
  gt(groupname_col = "sourceName") %>%
  fmt_number(3:11, decimals = 3) %>%
  cols_label(
    manualMean = "Mean",
    automatedMean = "Mean*",
    manualSd = "Sd",
    automatedSd = "Sd*",
    manualAbsErr = "Abs Error",
    automatedAbsErr = "Abs Error*",
    absErrorDiff = "Error difference",
    z = "Z-score",
    exposureName = ""
  ) %>%
  tab_options(row_group.background.color = "lightgrey"
  ) %>%
  tab_header("Manual and Automated Null Distributions for exposures") %>%
  tab_source_note("*Denotes automated method for selecting negative controls")

outputTable