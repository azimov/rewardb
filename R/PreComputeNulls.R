# Copyright Janssen Pharmacuiticals 2021 All rights reserved

outcomeNullDistsProc <- function(exposureIds, config, analysisId, minCohortSize = 10) {
  devtools::load_all()
  connection <- DatabaseConnector::connect(config$connectionDetails)
  on.exit(DatabaseConnector::disconnect(connection))
  nullData <- loadRenderTranslateQuerySql(connection,
                                          "calibration/outcomeCohortNullData.sql",
                                          cem = config$cemSchema,
                                          results_schema = config$rewardbResultsSchema,
                                          vocabulary_schema = config$vocabularySchema,
                                          min_cohort_size = minCohortSize,
                                          exposure_ids = exposureIds[!is.na(exposureIds)],
                                          analysis_id = analysisId,
                                          snakeCaseToCamelCase = TRUE)

  exposureRefSet <- unique(data.frame(exposureId = nullData$exposureId,
                                      outcomeType = nullData$outcomeType,
                                      sourceId = nullData$sourceId,
                                      targetCohortId = nullData$targetCohortId))

  getNullDist <- function(x) {
    oType <- ifelse(x["outcomeType"] == 1, 1, 0)
    negatives <- nullData[nullData$sourceId == x["sourceId"] & nullData$exposureId == x["exposureId"] & nullData$outcomeType == oType,]
    nullDist <- EmpiricalCalibration::fitNull(logRr = log(negatives$rr), seLogRr = negatives$seLogRr)
    absSysError <- EmpiricalCalibration::computeExpectedAbsoluteSystematicError(nullDist)
    results <- data.frame(sourceId = x["sourceId"],
                          analysisId = analysisId,
                          ingredientConceptId = x["exposureId"],
                          outcomeType = oType,
                          targetCohortId = x["targetCohortId"],
                          nullDistMean = nullDist["mean"],
                          nullDistSd = nullDist["sd"],
                          absoluteError = absSysError,
                          nControls = nrow(negatives))
  }

  rowsList <- apply(exposureRefSet, 1, getNullDist)
  rows <- do.call(rbind, rowsList)
  return(rows)
}

#' @title
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
  rows <- do.call(rbind, res)
  colnames(rows) <- SqlRender::camelCaseToSnakeCase(colnames(rows))
  pgCopyDataFrame(config$connectionDetails, rows, schema = config$rewardbResultsSchema, "outcome_null_distributions")
}

exposureNullDistsProc <- function(outcomeIds, config, analysisId, minCohortSize = 10) {
  devtools::load_all()
  connection <- DatabaseConnector::connect(config$connectionDetails)
  on.exit(DatabaseConnector::disconnect(connection))
  nullData <- loadRenderTranslateQuerySql(connection,
                                          "calibration/exposureCohortNullData.sql",
                                          cem = config$cemSchema,
                                          results_schema = config$rewardbResultsSchema,
                                          vocabulary = config$vocabularySchema,
                                          min_cohort_size = minCohortSize,
                                          outcome_ids = outcomeIds[!is.na(outcomeIds)],
                                          analysis_id = analysisId,
                                          snakeCaseToCamelCase = TRUE)

  exposureRefSet <- unique(data.frame(sourceId = nullData$sourceId,
                                      outcomeCohortId = nullData$outcomeCohortId))

  getNullDist <- function(x) {
    negatives <- nullData[nullData$sourceId == x["sourceId"] & nullData$outcomeCohortId == x["outcomeCohortId"],]
    nullDist <- EmpiricalCalibration::fitNull(logRr = log(negatives$rr), seLogRr = negatives$seLogRr)
    absSysError <- EmpiricalCalibration::computeExpectedAbsoluteSystematicError(nullDist)
    results <- data.frame(sourceId = x["sourceId"],
                          analysisId = analysisId,
                          outcomeCohortId = x["outcomeCohortId"],
                          nullDistMean = nullDist["mean"],
                          nullDistSd = nullDist["sd"],
                          absoluteError = absSysError,
                          nControls = nrow(negatives))
  }

  rowsList <- apply(exposureRefSet, 1, getNullDist)
  rows <- do.call(rbind, rowsList)
  return(rows)
}

#' @title
#' Compute null exposure distributions
#' @description
#' Compute the null exposure distribution for all exposure cohorts (requires results and cem_matrix summary table)
computeExposureNullDistributions <- function(config, analysisId = 1, nThreads = 10) {
  cluster <- ParallelLogger::makeCluster(nThreads)
  connection <- DatabaseConnector::connect(config$connectionDetails)
  on.exit({
    DatabaseConnector::disconnect(connection)
    ParallelLogger::stopCluster(cluster)
  }, add = TRUE)

  sql <- "SELECT cohort_definition_id as id FROM @schema.outcome_cohort_definition"
  outcomeIds <- DatabaseConnector::renderTranslateQuerySql(connection, sql, schema = config$rewardbResultsSchema)$ID

  nCuts <- nThreads
  if (nThreads == 1) {
    nCuts <- 2 # Useful to have multipe lists for IDs for testing logic
  }
  outcomeIdGroups <- split(outcomeIds, cut(seq_along(outcomeIds), nCuts, labels = FALSE))

  res <- ParallelLogger::clusterApply(cluster, outcomeIdGroups, exposureNullDistsProc, config = config, analysisId = analysisId)
  rows <- do.call(rbind, res)
  colnames(rows) <- SqlRender::camelCaseToSnakeCase(colnames(rows))
  pgCopyDataFrame(config$connectionDetails, rows, schema = config$rewardbResultsSchema, "exposure_null_distributions")
}