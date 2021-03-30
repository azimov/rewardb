devtools::load_all()
library(MethodEvaluation)
# Ad cohorts to results

config <- loadGlobalConfig("config/global-cfg.yml")
connection <- DatabaseConnector::connect(config$connectionDetails)

cdmConfigPaths <-c(
  #"config/cdm/pharmetrics.yml",
  "config/cdm/mcdc.yml",
  "config/cdm/mcdr.yml",
  "config/cdm/ccae.yml",
  "config/cdm/optum.yml"
)

configId <- "exposure-control-evaluation"

outcomes <- list()
adHocCohortIds <- c()


benchmarkOutcomeCohorts <- data.frame(sqlName = c("acute_pancreatitis", "gi_bleed", "stroke", "ibd"),
                                      name = c("[Benchmark] Acute Pancreatitis", "[Benchmark] GI Bleed", "[Benchmark] stroke", "[Benchmark] ibd"),
                                      cohortId = c(1, 2, 3, 4))

sourceUrl <- "OHDSI/MethodsLibrary"

for (i in 1:nrow(benchmarkOutcomeCohorts)) {
  ParallelLogger::logInfo(paste("Creating outcome:", benchmarkOutcomeCohorts$sqlName[i]))

  sqlFilename <- paste0(benchmarkOutcomeCohorts$sqlName[i], ".sql")
  
  sqlFile <- system.file(file.path("sql", "sql_server", sqlFilename), package = "MethodEvaluation")
  sqlDefinition <- readr::read_file(sqlFile)
  
  jsonFilename <- paste0(benchmarkOutcomeCohorts$sqlName[i], ".json")
  jsonFile <- system.file(file.path("cohorts", jsonFilename), package = "MethodEvaluation")
  cohortDefinition <- list()
  jsonDef <- jsonlite::read_json(jsonFile)
  
  cohortDefinition$name <- benchmarkOutcomeCohorts$name[i]
  cohortDefinition$description <- "OHDSI exposure negative control benchmark outcome - OHDSI/MethodEvaluation"
  cohortDefinition$id <- i
  cohortDefinition$expression <- jsonDef
  
  removeAtlasCohort(connection, config, i, webApiUrl = sourceUrl)
  
  insertAtlasCohortRef(connection,
                       config,
                       i,
                       webApiUrl= sourceUrl,
                       cohortDefinition = cohortDefinition,
                       sqlDefinition = sqlDefinition)
}


zipFilePath <- "rewardb-references.zip"
exportReferenceTables(config, exportZipFile = zipFilePath)

for (path in cdmConfigPaths) {
  cdmConfig <- loadCdmConfig(path)
  importReferenceTables(cdmConfig, zipFilePath = zipFilePath)
  resultsFiles <- sccAdHocCohorts(path, configId, c(1, 2, 3, 4), sourceUrl = sourceUrl)
  for (table in names(resultsFiles)) {
    for (sqlFile in resultsFiles[[table]]) {
      pgCopy(config$connectionDetails, sqlFile, config$rewardbResultsSchema, table)
    }
  }
}

# Get exposure controls for 4 outcomes
data("ohdsiNegativeControls", package = 'MethodEvaluation')
ohdsiNegativeControls <- ohdsiNegativeControls[ohdsiNegativeControls$type == "Exposure control",]

ohdsiControlsMapping <- data.frame(
  exposureConceptId = c(ohdsiNegativeControls$targetId, ohdsiNegativeControls$nestingId),
  atlasId = c(ohdsiNegativeControls$outcomeId, ohdsiNegativeControls$outcomeId),
  atlasUrl = sourceUrl
)


manualControlData <- getConceptCohortDataFromAtlasOutcomes(connection, config, ohdsiControlsMapping)
automatedControlsData <- getAtlasAutomatedExposureControlData(connection, config, atlasIds = 1:4, sourceUrl = sourceUrl)


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



results <- data.frame()
for (sourceId in c(10, 11, 12, 13)) {
  for (outcomeId in 1:4) {

    row <- getSetResults(manualControlData[manualControlData$sourceId == sourceId & manualControlData$atlasId == outcomeId,],
                         automatedControlsData[automatedControlsData$sourceId == sourceId & automatedControlsData$atlasId == outcomeId,],
                         paste0("extra/eval_results_exp/manual_plot_sid", sourceId, "-oid", outcomeId, ".png"),
                         paste0("extra/eval_results_exp/auto_plot_sid", sourceId, "-oid", outcomeId, ".png"))

    row$outcomeId <- outcomeId
    row$sourceId <- sourceId
    results <- rbind(results, row)
  }
}
saveRDS(results, "extra/exposureControlEvaluationTable.rds")
