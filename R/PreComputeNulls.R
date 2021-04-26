# Copyright Janssen Pharmacuiticals 2021 All rights reserved

exposureNullDistsProc <- function(exposureIds, config, analysisId) {
  devtools::load_all()
  connection <- DatabaseConnector::connect(config$connectionDetails)
  on.exit(DatabaseConnector::disconnect(connection))
  nullData <- loadRenderTranslateQuerySql(connection,
                              "calibration/exposureCohortNullData.sql",
                              cem = config$cemSchema,
                              results_schema = config$rewardbResultsSchema,
                              vocabulary_schema = config$vocabularySchema,
                              exposure_ids = exposureIds,
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
computeExposureNullDistributions <- function(connection, config, analysisId = 1, nThreads = 10) {

  sql <- "SELECT drug_conceptset_id as id FROM @schema.cohort_definition"
  exposureIds <- DatabaseConnector::renderTranslateQuerySql(connection, sql, schema = config$rewardbResultsSchema)$ID

  if (nThreads == 1) {
    # Useful to have multipe lists for IDs
    exposureIdGroups <- split(exposureIds, cut(seq_along(exposureIds), 2, labels = FALSE))
  } else {
    exposureIdGroups <- split(exposureIds, cut(seq_along(exposureIds), nThreads, labels = FALSE))
  }

  cluster <- ParallelLogger::makeCluster(nThreads)
  on.exit(ParallelLogger::stopCluster(cluster))

  res <- ParallelLogger::clusterApply(cluster, exposureIdGroups, exposureNullDistsProc, config = config, analysisId = analysisId)

  rows <- data.frame()
  for (row in res) {
    rows <- rbind(rows, row)
  }
  colnames(rows) <- SqlRender::camelCaseToSnakeCase(colnames(rows))
  readr::write_csv(rows, "exposure_null_distributions.csv") # TODO remove after testing is complete
  pgCopyDataFrame(config$connectionDetails, rows, schema = config$rewardbResultsSchema, "exposure_null_distributions")
}