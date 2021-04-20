devtools::load_all()
library(MethodEvaluation)
# Ad cohorts to results
generateCohorts <- FALSE
config <- loadGlobalConfig("config/global-cfg.yml")
connection <- DatabaseConnector::connect(config$connectionDetails)

cdmConfigPaths <- c(
  "config/cdm/jmdc.yml",
  "config/cdm/mcdc.yml",
  "config/cdm/mcdr.yml",
  "config/cdm/ccae.yml",
  "config/cdm/optum.yml"
)

configId <- "exposure-control-evaluation"

outcomes <- list()
adHocCohortIds <- c()

ibdAtlasEvidenceExport <- read.csv("extra/controls/ibd_evidence.csv")
ibdAtlasEvidence <- data.frame(atlas_id = 4,
                               cohort_definition_id = 345215,
                               ingredient_concept_id = ibdAtlasEvidenceExport$Id,
                               evidence_exists = as.integer(ibdAtlasEvidenceExport$`Suggested.Negative.Control` == "N"))


benchmarkOutcomeCohorts <- data.frame(sqlName = c("acute_pancreatitis", "gi_bleed", "stroke", "ibd"),
                                      name = c("[Benchmark] Acute Pancreatitis", "[Benchmark] GI Bleed", "[Benchmark] stroke", "[Benchmark] ibd"),
                                      cohortId = c(1, 2, 3, 4))

sourceUrl <- "OHDSI/MethodsLibrary"
if (generateCohorts) {
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
                         webApiUrl = sourceUrl,
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
automatedControlsData <- getAtlasAutomatedExposureControlData(connection, config, atlasIds = 1:4, sourceUrl = sourceUrl, extraEvidence = ibdAtlasEvidence)

outcomeCohortIds <- unique(manualControlData$outcomeCohortId)
sql <- "SELECT sr.*, aor.atlas_id FROM @schema.scc_result sr
INNER JOIN @schema.atlas_outcome_reference aor on sr.outcome_cohort_id = aor.cohort_definition_id
WHERE sr.outcome_cohort_id IN (@outcome_cohort_ids) AND sr.RR > 0 AND sr.t_cases + sr.c_cases > 10"
fullDataSet <- DatabaseConnector::renderTranslateQuerySql(connection, sql, outcome_cohort_ids = outcomeCohortIds, schema = config$rewardbResultsSchema, snakeCaseToCamelCase = TRUE)

percentSignificant <- function(nullDist, positives, rrThresh = 0.5, pThresh = 0.05) {
  # Get fraction of exposures signficant for causing or preventing outcome after calibration

  pValues <- EmpiricalCalibration::calibrateP(nullDist, log(positives$RR), positives$SE_LOG_RR)
  errorModel <- EmpiricalCalibration::convertNullToErrorModel(nullDist)
  ci <- EmpiricalCalibration::calibrateConfidenceInterval(log(positives$RR), positives$SE_LOG_RR, errorModel)
  
  # confidence interval should not cross 1, signficant p value and rr value
  nSignificantPost <- sum((ci$logUb95Rr > 0 & ci$logLb95Rr > 0) | (ci$logUb95Rr < 0 & ci$logLb95Rr < 0) & ci$logRr < log(rrThresh) & pValues < pThresh)
}

getSetResults <- function(manualControlData, automatedControlsData, positives, manualFilename, automatedFilename) {

  EmpiricalCalibration::plotCalibrationEffect(log(manualControlData$rr), manualControlData$seLogRr, fileName = manualFilename, showExpectedAbsoluteSystematicError = TRUE)
  EmpiricalCalibration::plotCalibrationEffect(log(automatedControlsData$rr), automatedControlsData$seLogRr, fileName = automatedFilename, showExpectedAbsoluteSystematicError = TRUE)

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
  rrThresh <- 0.5
  pThresh <- 0.05
  data.frame(manualMean = mu1,
             manualSd = sd1,
             automatedMean = mu2,
             automatedSd = sd2,
             nAutoControls = nrow(automatedControlsData),
             z = z,
             p = p,
             manualAbsErr = mErr,
             automatedAbsErr = aErr,
             absErrorDiff = abs(mErr - aErr),
             nSignificantPre = sum((positives$ub95Rr > 1 & positives$lb95Rr > 1) | (positives$ub95Rr < 1 & positives$lb95Rr < 1) & positives$logRr < log(rrThresh) & positives$pValue < pThresh),
             nSignificantPostManual = percentSignificant(manualNullDist, positives),
             nSignificantPostAuto = percentSignificant(automatedNullDist, positives))

}


results <- data.frame()
for (sourceId in c(10, 11, 12, 13)) {
  for (outcomeId in 1:4) {
    row <- getSetResults(manualControlData[manualControlData$sourceId == sourceId & manualControlData$atlasId == outcomeId,],
                         automatedControlsData[automatedControlsData$sourceId == sourceId & automatedControlsData$atlasId == outcomeId,],
                         fullDataSet[fullDataSet$sourceId == sourceId & fullDataSet$atlasId == outcomeId, ],
                         paste0("extra/eval_results_exp/manual_plot_sid", sourceId, "-oid", outcomeId, ".png"),
                         paste0("extra/eval_results_exp/auto_plot_sid", sourceId, "-oid", outcomeId, ".png"))

    row$outcomeId <- outcomeId
    row$sourceId <- sourceId
    results <- rbind(results, row)
  }
}
saveRDS(results, "extra/exposureControlEvaluationTable.rds")

dataSources <- DatabaseConnector::renderTranslateQuerySql(connection,
                                                          "SELECT * FROM @schema.data_source",
                                                          schema = config$rewardbResultsSchema,
                                                          snakeCaseToCamelCase = TRUE)

cohortNames <- data.frame(name = c("Acute Pancreatitis", "GI Bleed", "Stroke", "ibd"), cohortId = c(1, 2, 3, 4))

outputTable <- results %>%
  inner_join(dataSources, by = "sourceId") %>%
  inner_join(cohortNames, by = c("outcomeId" = "cohortId")) %>%
  select(sourceName, name, manualMean, manualSd, automatedMean, automatedSd, manualAbsErr, automatedAbsErr, absErrorDiff, z, p) %>%
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
    name = ""
  ) %>%
  tab_options(row_group.background.color = "lightgrey"
  ) %>%
  tab_header("Manual and Automated Null Distributions for exposures") %>%
  tab_source_note("*Denotes automated method for selecting negative controls")

outputTable
