library(MethodEvaluation)

for (cdmConfigPath in Sys.glob("config/cdm/ccae.yml")) {
}
{
  config <- loadCdmConfig(cdmConfigPath)

  cdmDatabaseSchema <- config$cdmSchema
  oracleTempSchema <- NULL
  outcomeDatabaseSchema <- config$resultSchema
  outcomeTable <- "ohdsi_outcomes"
  nestingCohortDatabaseSchema <- config$resultSchema
  nestingCohortTable <- "ohdsi_nesting_cohorts"
  outputFolder <- "benchmarkOutput"
  cdmVersion <- "5"

  createReferenceSetCohorts(connectionDetails = config$connectionDetails,
                            oracleTempSchema = oracleTempSchema,
                            cdmDatabaseSchema = cdmDatabaseSchema,
                            outcomeDatabaseSchema = outcomeDatabaseSchema,
                            outcomeTable = outcomeTable,
                            nestingDatabaseSchema = nestingCohortDatabaseSchema,
                            nestingTable = nestingCohortTable,
                            referenceSet = "ohdsiMethodsBenchmark")

  synthesizeReferenceSetPositiveControls(connectionDetails = config$connectionDetails,,
                                         oracleTempSchema = oracleTempSchema,
                                         cdmDatabaseSchema = cdmDatabaseSchema,
                                         outcomeDatabaseSchema = outcomeDatabaseSchema,
                                         outcomeTable = outcomeTable,
                                         maxCores = 10,
                                         workFolder = outputFolder,
                                         summaryFileName = file.path(outputFolder,
                                                                     "allControls.csv"),
                                         referenceSet = "ohdsiMethodsBenchmark")

  library(SelfControlledCohort)
  runSccArgs1 <- createRunSelfControlledCohortArgs(addLengthOfExposureExposed = TRUE,
                                                   riskWindowStartExposed = 0,
                                                   riskWindowEndExposed = 0,
                                                   riskWindowEndUnexposed = -1,
                                                   addLengthOfExposureUnexposed = TRUE,
                                                   riskWindowStartUnexposed = -1,
                                                   washoutPeriod = 365)
  sccAnalysis1 <- createSccAnalysis(analysisId = 1,
                                    description = "Length of exposure",
                                    runSelfControlledCohortArgs = runSccArgs1)
  runSccArgs2 <- createRunSelfControlledCohortArgs(addLengthOfExposureExposed = FALSE,
                                                   riskWindowStartExposed = 0,
                                                   riskWindowEndExposed = 30,
                                                   riskWindowEndUnexposed = -1,
                                                   addLengthOfExposureUnexposed = FALSE,
                                                   riskWindowStartUnexposed = -30,
                                                   washoutPeriod = 365)
  sccAnalysis2 <- createSccAnalysis(analysisId = 2,
                                    description = "30 days of each exposure",
                                    runSelfControlledCohortArgs = runSccArgs2)
  sccAnalysisList <- list(sccAnalysis1, sccAnalysis2)

  allControls <- read.csv(file.path(outputFolder , "allControls.csv"))

  eos <- list()
  for (i in 1:nrow(allControls)) {
    eos[[length(eos) + 1]] <- createExposureOutcome(exposureId = allControls$targetId[i],
                                                    outcomeId = allControls$outcomeId[i])
  }

  sccResult <- runSccAnalyses(connectionDetails = connectionDetails,
                            cdmDatabaseSchema = cdmDatabaseSchema,
                            oracleTempSchema = oracleTempSchema,
                            exposureTable = "drug_era",
                            outcomeDatabaseSchema = outcomeDatabaseSchema,
                            outcomeTable = outcomeTable,
                            sccAnalysisList = sccAnalysisList,
                            exposureOutcomeList = eos,
                            outputFolder = outputFolder)

  sccSummary <- summarizeAnalyses(sccResult, outputFolder)
  write.csv(sccSummary, file.path(outputFolder, "sccSummary.csv"), row.names = FALSE)

  estimates <- readRDS(file.path(outputFolder, "sccSummary.csv"))
  estimates <- data.frame(analysisId = estimates$analysisId,
                          targetId = estimates$exposureId,
                          outcomeId = estimates$outcomeId,
                          logRr = estimates$logRr,
                          seLogRr = estimates$seLogRr,
                          ci95Lb = estimates$irrLb95,
                          ci95Ub = estimates$irrUb95)

  analysisRef <- data.frame(method = "SelfControlledCohort",
                          analysisId = c(1, 2),
                          description = c("Length of exposure",
                                          "30 days of each exposure"),
                          details = "",
                          comparative = FALSE,
                          nesting = FALSE,
                          firstExposureOnly = FALSE)

  exportFolder <- file.path(outputFolder, "export")
  metrics <- computeOhdsiBenchmarkMetrics(exportFolder,
                                          mdrr = 1.25,
                                          stratum = "All",
                                          trueEffectSize = "Overall",
                                          calibrated = FALSE,
                                          comparative = FALSE)
}