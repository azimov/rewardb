devtools::load_all()
library(dplyr)
library(gt)
source("scripts/controlEvaluationFunctions.R")
config <- loadGlobalConfig("config/global-cfg.yml")
connection <- DatabaseConnector::connect(config$connectionDetails)

dataSources <- DatabaseConnector::renderTranslateQuerySql(connection, "SELECT * FROM @schema.data_source", schema=config$rewardbResultsSchema, snakeCaseToCamelCase = TRUE)

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
automatedControlsData <- automatedControlsData[automatedControlsData$tCases + automatedControlsData$cCases > 10, ]

exposureNames <- DatabaseConnector::renderTranslateQuerySql(connection,
                                                            "SELECT cohort_definition_id, drug_conceptset_id as exposure_id, cohort_definition_name as exposure_name FROM @schema.cohort_definition WHERE drug_conceptset_id in (@exposure_ids)",
                                                            schema = config$rewardbResultsSchema,
                                                            exposure_ids = unique(exposureOutcomePairs$exposureConceptId),
                                                            snakeCaseToCamelCase = TRUE)

sql <- "SELECT sr.*, cd.drug_conceptset_id as exposure_concept_id FROM @schema.scc_result sr
INNER JOIN @schema.cohort_definition cd ON sr.target_cohort_id = cd.cohort_definition_id
WHERE sr.target_cohort_id IN (@target_cohort_ids) AND sr.RR > 0 AND sr.t_cases + sr.c_cases > 10"
fullDataSet <- DatabaseConnector::renderTranslateQuerySql(connection, sql, target_cohort_ids = exposureNames$cohortDefinitionId, schema = config$rewardbResultsSchema, snakeCaseToCamelCase = TRUE)
fullDataSet$logUb95Rr <- log(fullDataSet$ub95)
fullDataSet$logLb95Rr <- log(fullDataSet$lb95)

plotList <- list()
results <- data.frame()
errorRates <- data.frame()
fullDataSet$logUb95Rr <- log(fullDataSet$ub95)
fullDataSet$logLb95Rr <- log(fullDataSet$lb95)

for (sourceId in c(10, 11, 12, 13)) {

  plotList[[sourceId]] <- list()
  for (exposureId in unique(exposureOutcomePairs$exposureConceptId)) {

    mData <- manualControlData[manualControlData$sourceId == sourceId & manualControlData$exposureConceptId == exposureId,]
    autData <- automatedControlsData[automatedControlsData$sourceId == sourceId & automatedControlsData$exposureConceptId == exposureId,]
    positives <- fullDataSet[fullDataSet$sourceId == sourceId & fullDataSet$exposureConceptId == exposureId,]

    manualNullDist <- EmpiricalCalibration::fitNull(logRr = log(mData$rr), seLogRr = mData$seLogRr)
    automatedNullDist <- EmpiricalCalibration::fitNull(logRr = log(autData$rr), seLogRr = autData$seLogRr)
    ename <- exposureNames[exposureNames$exposureId == exposureId, ]$exposureName
    plotList[[sourceId]][[ename]] <- getCalibrationPlots(mData, autData,
                                 paste0("extra/eval_results/manual_plot_sid", sourceId, "-oid", outcomeId, ".png"),
                                 paste0("extra/eval_results/auto_plot_sid", sourceId, "-oid", outcomeId, ".png"))

    row <- getSetResults(manualNullDist, automatedNullDist, autData)

    autoCalibratedRows <- calibratedRows(automatedNullDist, positives)
    manualCalibratedRows <- calibratedRows(manualNullDist, positives)

    row$exposureId <- exposureId
    row$sourceId <- sourceId
    results <- rbind(results, row)

    setEvalUncalibrated <- setEvaluation(manualCalibratedRows, positives, "outcomeCohortId")
    setEvalCalibrated <- setEvaluation(manualCalibratedRows, autoCalibratedRows, "outcomeCohortId")

    setEvalCalibrated$exposureId <- exposureId
    setEvalCalibrated$sourceId <- sourceId
    setEvalCalibrated$type <- "Calibrated"
    setEvalCalibrated$manualAbsErr <- row$manualAbsErr

    setEvalUncalibrated$exposureId <- exposureId
    setEvalUncalibrated$sourceId <- sourceId
    setEvalUncalibrated$type <- "Uncalibrated"
    setEvalUncalibrated$manualAbsErr <- row$manualAbsErr

    errorRates <- rbind(errorRates, setEvalCalibrated, setEvalUncalibrated)
  }
}

saveRDS(results, "extra/outcomeControlEvaluationTable.rds")


barDt <- rbind(
  data.frame(
    type = "manual",
    sourceId = results$sourceId,
    exposureId = results$exposureId,
    absErr = results$manualAbsErr
  ),
  data.frame(
    type = "auto",
    sourceId = results$sourceId,
    exposureId = results$exposureId,
    absErr = results$automatedAbsErr
  )
)

barDt <- barDt %>% inner_join(dataSources) %>% inner_join(exposureNames)

barPlot <- ggplot2::ggplot(barDt, ggplot2::aes(x=sourceName, y=absErr, fill=type)) + 
  ggplot2::geom_bar(stat = "identity", position=ggplot2::position_dodge()) +
  ggplot2::xlab("") + ggplot2::ylab("Expected Absolute Systematic Error") +
  ggplot2::facet_wrap(~exposureName) + ggplot2::theme(text = ggplot2::element_text(size=18))
  
  
for (exposure in names(plotList[[10]])) {
  plot <- outcomeByDbCalibrationPlots(plotList, exposure)
  ggplot2::ggsave(paste("outcomeControls-", exposure, ".png"), plot)
}


dataSources <- DatabaseConnector::renderTranslateQuerySql(connection,
                                                          "SELECT * FROM @schema.data_source",
                                                          schema = config$rewardbResultsSchema,
                                                          snakeCaseToCamelCase = TRUE)

# Create fan plots
for (sourceId  in c(10, 11, 12, 13)) {
  name <- dataSources[dataSources$sourceId == sourceId, ]$sourceName
  gridPlot <- outcomeCalibrationPlotGrid(plotList[[paste0(sourceId)]], name)
  filename <- file.path("extra", "eval_results", paste0(name, "-outcome-calibration-plots.png"))
  ggplot2::ggsave(filename, gridPlot)
}

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


sql <- "SELECT * FROM @schema.outcome_control_distribution WHERE outcome_type = 0"
nullDists <- DatabaseConnector::renderTranslateQuerySql(connection, sql, schema= config$rewardbResultsSchema, snakeCaseToCamelCase = TRUE)

plot <- ggplot2::ggplot(nullDists[nullDists$sourceId != 15,], ggplot2::aes(x=sourceName, y=absoluteError, color=sourceName)) +
  ggplot2::geom_violin() + 
  ggplot2::geom_boxplot(width = 0.2) + 
  ggplot2::xlab("Data Source") + 
  ggplot2::ylab("Expected Absolute Systematic Error") +
  ggplot2::theme(legend.position = "none", text = ggplot2::element_text(size=15))

ggplot2::ggplot(nullDists, ggplot2::aes(x=sourceId, y=absoluteError)) + ggplot2::geom_violin()