percentSignificant <- function(nullDist, positives, pThresh = 0.05) {
  # Get fraction of exposures signficant for causing or preventing outcome after calibration

  pValues <- EmpiricalCalibration::calibrateP(nullDist, log(positives$rr), positives$seLogRr)
  errorModel <- EmpiricalCalibration::convertNullToErrorModel(nullDist)
  ci <- EmpiricalCalibration::calibrateConfidenceInterval(log(positives$rr), positives$seLogRr, errorModel)

  # confidence interval should not cross 1, signficant p value and rr value
  nSignificantPost <- sum((ci$logUb95Rr > 0 & ci$logLb95Rr > 0) | (ci$logUb95Rr < 0 & ci$logLb95Rr < 0) & pValues < pThresh)
}


getCalibrationPlots <- function(manualControlData, automatedControlsData, positives, manualFilename, automatedFilename) {
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
                          ncol = 3, nrow = 5, widths = c(1, 3, 3), heights = c(4,4,4,4, 1), bottom = paste(title, "Exposure controls"))
}

# False positive rate
typeIerrorRateCount <- function(goldStandard, comparator) {
  truePositives <- goldStandard[(goldStandard$ub95Rr > 1 & goldStandard$lb95Rr > 1) | (goldStandard$ub95Rr < 1 & goldStandard$lb95Rr < 1) & goldStandard$pValue <= 0.05, ]$exposureId
  testPositives <- comparator[((comparator$ub95Rr > 1 & comparator$lb95Rr > 1) | (comparator$ub95Rr < 1 & comparator$lb95Rr < 1) & comparator$pValue <= 0.05), ]$exposureId

  sum(!(testPositives %in% truePositives))
}

# False negative rate
typeIIerrorCount <- function(goldStandard, comparator) {
  trueNegatives <- goldStandard[!((goldStandard$ub95Rr > 1 & goldStandard$lb95Rr > 1) | (goldStandard$ub95Rr < 1 & goldStandard$lb95Rr < 1) & goldStandard$pValue <= 0.05), ]$exposureId
  testNegatives <- comparator[!((comparator$ub95Rr > 1 & comparator$lb95Rr > 1) | (comparator$ub95Rr < 1 & comparator$lb95Rr < 1) & comparator$pValue <= 0.05), ]$exposureId

  sum(!(testNegatives %in% trueNegatives))
}

getSetResults <- function(manualControlData, automatedControlsData, positives) {

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
  pThresh <- 0.05


  data.frame(manualMean = mu1,
             manualSd = sd1,
             automatedMean = mu2,
             automatedSd = sd2,
             z = z,
             p = p,
             manualAbsErr = mErr,
             automatedAbsErr = aErr,
             absErrorDiff = abs(mErr - aErr),
             nAutoControls = nrow(automatedControlsData),
             nSignificantPre = sum((positives$ub95Rr > 1 & positives$lb95Rr > 1) | (positives$ub95Rr < 1 & positives$lb95Rr < 1) & positives$pValue < pThresh),
             nSignificantPostManual = percentSignificant(manualNullDist, positives),
             nSignificantPostAuto = percentSignificant(automatedNullDist, positives))

}