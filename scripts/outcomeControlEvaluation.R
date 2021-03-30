devtools::load_all()
library(dplyr)
config <- loadGlobalConfig("config/global-cfg.yml")
connection <- DatabaseConnector::connect(config$connectionDetails)


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

  data.frame(manualMean = mu1,
             manualSd = sd1,
             automatedMean = mu2,
             automatedSd = sd2,
             z = z,
             p = p,
             manualAbsErr = EmpiricalCalibration::computeExpectedAbsoluteSystematicError(manualNullDist),
             automatedAbsErr = EmpiricalCalibration::computeExpectedAbsoluteSystematicError(automatedNullDist))

}


# Map exposure to reward cohorts (cohort id)
data("ohdsiNegativeControls", package = 'MethodEvaluation')
ohdsiNegativeControls <- ohdsiNegativeControls[ohdsiNegativeControls$type != "Exposure control",]

exposureOutcomePairs <- data.frame(exposureConceptId = ohdsiNegativeControls$targetId, outcomeConceptId = ohdsiNegativeControls$outcomeId)
manualControlData <- getConcetptCohortData(connection, config, exposureOutcomePairs)
saveRDS(manualControlData, "extra/outcomeControlEvaluationManualControlsDt.rds")


automatedExposureOutcomePairs <- getOutcomeControlConcepts(connection, config, unique(exposureOutcomePairs$exposureConceptId))
automatedControlsData <- getConcetptCohortData(connection, config, automatedExposureOutcomePairs)
saveRDS(automatedControlsData, "extra/outcomeControlEvaluationAutomatedControlsDt.rds")


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
