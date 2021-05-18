percentSignificant <- function(nullDist, positives, pThresh = 0.05) {
  # Get fraction of exposures signficant for causing or preventing outcome after calibration

  pValues <- EmpiricalCalibration::calibrateP(nullDist, log(positives$rr), positives$seLogRr)
  errorModel <- EmpiricalCalibration::convertNullToErrorModel(nullDist)
  ci <- EmpiricalCalibration::calibrateConfidenceInterval(log(positives$rr), positives$seLogRr, errorModel)

  # confidence interval should not cross 1, signficant p value and rr value
  nSignificantPost <- sum((ci$logUb95Rr > 0 & ci$logLb95Rr > 0) | (ci$logUb95Rr < 0 & ci$logLb95Rr < 0) & pValues < pThresh)
}


calibratedRows <- function(nullDist, uncalibratedData) {
  pValues <- EmpiricalCalibration::calibrateP(nullDist, log(uncalibratedData$rr), uncalibratedData$seLogRr)
  errorModel <- EmpiricalCalibration::convertNullToErrorModel(nullDist)
  ci <- EmpiricalCalibration::calibrateConfidenceInterval(log(uncalibratedData$rr), uncalibratedData$seLogRr, errorModel)

  calibratedData <- data.frame(uncalibratedData)
  calibratedData$pValue <- pValues
  calibratedData$logLb95Rr <- ci$logLb95Rr
  calibratedData$logUb95Rr <- ci$logUb95Rr

  return(calibratedData)
}


getCalibrationPlots <- function(manualControlData, automatedControlsData, manualFilename, automatedFilename) {
  list(
    manualPlot = EmpiricalCalibration::plotCalibrationEffect(log(manualControlData$rr), manualControlData$seLogRr, fileName = manualFilename, showExpectedAbsoluteSystematicError = TRUE),
    autoPlot = EmpiricalCalibration::plotCalibrationEffect(log(automatedControlsData$rr), automatedControlsData$seLogRr, fileName = automatedFilename, showExpectedAbsoluteSystematicError = TRUE)
  )
}

exposureCalibrationPlotGrid <- function(plots, title) {
  gridExtra::grid.arrange(grid::textGrob("Acute\nPancreatitis"),
                          plots[[1]]$manualPlot,
                          plots[[1]]$autoPlot,
                          grid::textGrob("GI Bleed"),
                          plots[[2]]$manualPlot,
                          plots[[2]]$autoPlot,

                          grid::textGrob("Stroke"),
                          plots[[3]]$manualPlot,
                          plots[[3]]$autoPlot,

                          grid::textGrob("IBD"),
                          plots[[4]]$manualPlot,
                          plots[[4]]$autoPlot,

                          grid::textGrob(""),
                          grid::textGrob("Manual"),
                          grid::textGrob("Automatic"),
                          ncol = 3, nrow = 5, widths = c(1, 3, 3), heights = c(4, 4, 4, 4, 1), bottom = paste(title, "Exposure controls"))
}

outcomeCalibrationPlotGrid <- function(plots, title) {
  gridExtra::grid.arrange(grid::textGrob("diclofenac"),
                          plots$diclofenac$manualPlot,
                          plots$diclofenac$autoPlot,

                          grid::textGrob("ciprofloxacin"),
                          plots$ciprofloxacin$manualPlot,
                          plots$ciprofloxacin$autoPlot,

                          grid::textGrob("metformin"),
                          plots$metformin$manualPlot,
                          plots$metformin$autoPlot,

                          grid::textGrob("sertraline"),
                          plots$sertraline$manualPlot,
                          plots$sertraline$autoPlot,

                          grid::textGrob(""),
                          grid::textGrob("Manual"),
                          grid::textGrob("Automatic"),
                          ncol = 3, nrow = 5, widths = c(1, 3, 3), heights = c(4, 4, 4, 4, 1), bottom = paste(title, "Outcome controls"))
}


outcomeByDbCalibrationPlots <- function(plots, exposure) {
  gridExtra::grid.arrange(grid::textGrob("Manual"),
                          plots[[10]][[exposure]]$manualPlot,
                          plots[[11]][[exposure]]$manualPlot,
                          plots[[12]][[exposure]]$manualPlot,
                          plots[[13]][[exposure]]$manualPlot,

                          grid::textGrob("Auto"),
                          plots[[10]][[exposure]]$autoPlot,
                          plots[[11]][[exposure]]$autoPlot,
                          plots[[12]][[exposure]]$autoPlot,
                          plots[[13]][[exposure]]$autoPlot,

                          grid::textGrob(""),
                          grid::textGrob("Optum SES"),
                          grid::textGrob("IBM CCAE"),
                          grid::textGrob("IBM MDCD"),
                          grid::textGrob("IBM MDCR"),
                          ncol = 5, nrow = 3, widths=c(1, 4,4,4,4), heights=c(5,5,1), bottom = paste(exposure, "outcome controls"))
}

setEvaluation <- function(goldStandard, comparator, uIdField) {

  gsPositives <- goldStandard[(goldStandard$logUb95Rr > 0 & goldStandard$logLb95Rr > 0) | (goldStandard$logUb95Rr < 0 & goldStandard$logLb95Rr < 0) & goldStandard$pValue <= 0.05,][[uIdField]]
  gsNegatives <- goldStandard[!((goldStandard$logUb95Rr > 0 & goldStandard$logLb95Rr > 0) | (goldStandard$logUb95Rr < 0 & goldStandard$logLb95Rr < 0) & goldStandard$pValue <= 0.05),][[uIdField]]
  comparatorPositives <- comparator[(comparator$logUb95Rr > 0 & comparator$logLb95Rr > 0) | (comparator$logUb95Rr < 0 & comparator$logLb95Rr < 0) & comparator$pValue <= 0.05,][[uIdField]]
  comparatorNegatives <- comparator[!((comparator$logUb95Rr > 0 & comparator$logLb95Rr > 0) | (comparator$logUb95Rr < 0 & comparator$logLb95Rr < 0) & comparator$pValue <= 0.05),][[uIdField]]

  tp <- sum(comparatorPositives %in% gsPositives)
  fp <- sum(!(comparatorPositives %in% gsPositives))
  tn <- sum(comparatorNegatives %in% gsNegatives)
  fn <- sum(!(comparatorNegatives %in% gsNegatives))

  data.frame(sensetivity = tp / (tp + fn),
             specificity = tn / (tn + fp),
             ppv = tp / (tp + fp),
             npv = tn / (tn + fn),
             recall = tp/ (tp + fn),
             fnr = fn/(tn + fn),
             fp = fp,
             tn = tn,
             tp = tp)
}

getSetResults <- function(manualNullDist, automatedNullDist, automatedControlsData) {

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
             absErrorDiff = abs(mErr - aErr),
             nAutoControls = nrow(automatedControlsData))

}