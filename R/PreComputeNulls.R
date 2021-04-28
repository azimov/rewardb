# Copyright Janssen Pharmacuiticals 2021 All rights reserved

outcomeNullDistsProc <- function(exposureIds, config, analysisId) {
  devtools::load_all()
  connection <- DatabaseConnector::connect(config$connectionDetails)
  on.exit(DatabaseConnector::disconnect(connection))
  nullData <- loadRenderTranslateQuerySql(connection,
                                          "calibration/outcomeCohortNullData.sql",
                                          cem = config$cemSchema,
                                          results_schema = config$rewardbResultsSchema,
                                          vocabulary_schema = config$vocabularySchema,
                                          exposure_ids = exposureIds[!is.na(exposureIds)],
                                          analysis_id = analysisId,
                                          snakeCaseToCamelCase = TRUE)

  exposureRefSet <- unique(data.frame(exposureId = nullData$exposureId, sourceId = nullData$sourceId, targetCohortId = nullData$targetCohortId))

  getNullDist <- function(x) {
    negatives <- nullData[nullData$sourceId == x["sourceId"] & nullData$exposureId == x["exposureId"],]
    nullDist <- EmpiricalCalibration::fitNull(logRr = log(negatives$rr), seLogRr = negatives$seLogRr)
    absSysError <- EmpiricalCalibration::computeExpectedAbsoluteSystematicError(nullDist)
    results <- data.frame(sourceId = x["sourceId"],
                          analysisId = analysisId,
                          ingredientConceptId = x["exposureId"],
                          targetCohortId = x["targetCohortId"],
                          nullDistMean = nullDist["mean"],
                          nullDistSd = nullDist["sd"],
                          absoluteError = absSysError,
                          nControls = nrow(negatives))
  }

  rowsList <- apply(exposureRefSet, 1, getNullDist)

  rows <- data.frame()
  for (row in rowsList) {
    rows <- rbind(rows, row)
  }
  return(rows)
}

#'@tilte
#' Compute null exposure distributions
#' @description
#' Compute the null exposure distribution for all exposure cohorts (requires results and cem_matrix summary table)
computeOutcomeNullDistributions <- function(config, analysisId = 1, nThreads = 10) {
  cluster <- ParallelLogger::makeCluster(nThreads)
  connection <- DatabaseConnector::connect(config$connectionDetails)
  on.exit({
    DatabaseConnector::disconnect(connection)
    ParallelLogger::stopCluster(cluster)
  }, add = TRUE)

  sql <- "SELECT cohort_definition_id as id FROM @schema.cohort_definition"
  exposureIds <- DatabaseConnector::renderTranslateQuerySql(connection, sql, schema = config$rewardbResultsSchema)$ID

  nCuts <- nThreads
  if (nThreads == 1) {
    nCuts <- 2 # Useful to have multipe lists for IDs for testing logic
  }
  exposureIdGroups <- split(exposureIds, cut(seq_along(exposureIds), nCuts, labels = FALSE))

  res <- ParallelLogger::clusterApply(cluster, exposureIdGroups, outcomeNullDistsProc, config = config, analysisId = analysisId)

  rows <- data.frame()
  for (row in res) {
    rows <- unique(rbind(rows, row))
  }
  colnames(rows) <- SqlRender::camelCaseToSnakeCase(colnames(rows))
  pgCopyDataFrame(config$connectionDetails, rows, schema = config$rewardbResultsSchema, "outcome_null_distributions")
}