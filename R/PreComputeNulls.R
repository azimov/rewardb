getCemConnection <- function() {
  connectionDetails <- DatabaseConnector::createConnectionDetails(dbms = Sys.getenv("CEM_DATABASE_DBMS"),
                                                                  server = Sys.getenv("CEM_DATABASE_SERVER"),
                                                                  user = Sys.getenv("CEM_DATABASE_USER"),
                                                                  port = Sys.getenv("CEM_DATABASE_PORT"),
                                                                  extraSettings = Sys.getenv("CEM_DATABASE_EXTRA_SETTINGS"),
                                                                  password = keyring::key_get(Sys.getenv("CEM_KEYRING_SERVICE"),
                                                                                              user = Sys.getenv("CEM_DATABASE_USER")))
  CemConnector::CemDatabaseBackend$new(connectionDetails,
                                       cemSchema = Sys.getenv("CEM_DATABASE_SCHEMA"),
                                       vocabularySchema = Sys.getenv("CEM_DATABASE_VOCAB_SCHEMA"),
                                       sourceSchema = Sys.getenv("CEM_DATABASE_INFO_SCHEMA"))
}

#' Gets negative control outcome concepts for exposure cohorts
getCemExposureNegativeControlConcepts <- function(globalConfig) {
  cemConnection <- getCemConnection()
  on.exit(cemConnection$connection$closeConnection(), add = TRUE)
  globalConfig$useConnectionPool <- FALSE
  model <- ReportDbModel(globalConfig)
  sql <- "
  SELECT DISTINCT
    cd.cohort_definition_id as target_cohort_id,
    c.concept_id,
    c.IS_EXCLUDED,
    c.INCLUDE_DESCENDANTS
  from @schema.concept_set_definition c
  INNER JOIN @schema.cohort_definition cd ON cd.DRUG_CONCEPTSET_ID = c.conceptset_id
  INNER JOIN @schema.scc_result r ON r.target_cohort_id = cd.cohort_definition_id
  WHERE rr IS NOT NULL"
  exposureCohortConceptSets <- model$queryDb(sql, snakeCaseToCamelCase = TRUE)
  # Get mapped negative control evidence, keeps targetCohortId
  negativControlConcepts <- exposureCohortConceptSets %>%
    dplyr::group_by(targetCohortId) %>%
    dplyr::group_modify(~cemConnection$getSuggestedControlCondtions(.x), keep = TRUE) %>%
    dplyr::mutate(cohortDefinitionId = targetCohortId) %>%
    dplyr::select(cohortDefinitionId, conceptId)

  connection <- DatabaseConnector::connect(globalConfig$connectionDetails)
  on.exit(DatabaseConnector::disconnect(connection), add = TRUE)
  # Insert negative control concept sets
  DatabaseConnector::insertTable(connection,
                                 data = negativControlConcepts,
                                 databaseSchema = globalConfig$rewardbResultsSchema,
                                 tableName = "exposure_negative_control_concept",
                                 dropTableIfExists = TRUE,
                                 camelCaseToSnakeCase = TRUE,
                                 createTable = TRUE)
}


# Copyright Janssen Pharmacuiticals 2021 All rights reserved
outcomeNullDistsProc <- function(nullData) {

  nullDist <- EmpiricalCalibration::fitNull(logRr = log(nullData$rr), seLogRr = nullData$seLogRr)
  absSysError <- EmpiricalCalibration::computeExpectedAbsoluteSystematicError(nullDist)
  results <- data.frame(sourceId = nullData$sourceId[1],
                        analysisId = nullData$analysisId[1],
                        outcomeType = nullData$outcomeType[1],
                        targetCohortId = nullData$targetCohortId[1],
                        ingredientConceptId = nullData$ingredientConceptId[1],
                        nullDistMean = nullDist["mean"],
                        nullDistSd = nullDist["sd"],
                        absoluteError = absSysError,
                        nControls = nrow(nullData))

  return(results)
}

#' @title
#' Compute null exposure distributions
#' @description
#' Compute the null exposure distribution for all exposure cohorts (requires results and cem_matrix summary table)
computeOutcomeNullDistributions <- function(config, analysisId = 1, sourceIds = NULL, nThreads = 10, minCohortSize = 5) {
  cluster <- ParallelLogger::makeCluster(nThreads)
  connection <- DatabaseConnector::connect(config$connectionDetails)
  on.exit({
    DatabaseConnector::disconnect(connection)
    ParallelLogger::stopCluster(cluster)
  }, add = TRUE)

  nullData <- loadRenderTranslateQuerySql(connection,
                                          "calibration/outcomeCohortNullData.sql",
                                          results_schema = config$rewardbResultsSchema,
                                          min_cohort_size = minCohortSize,
                                          analysis_id = analysisId,
                                          source_ids = sourceIds,
                                          snakeCaseToCamelCase = TRUE)

  # Cut in to group by exposure id, source id, analysis id
  nullData <- nullData %>% dplyr::group_split(sourceId, analysisId, targetCohortId, outcomeType)
  res <- ParallelLogger::clusterApply(cluster, nullData, outcomeNullDistsProc)
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
#' Compute the null exposure distribution for all exposure cohorts
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
  outcomeIdGroups <- split(outcomeIds, cut(seq_along(outcomeIds), nCuts * 10, labels = FALSE))

  res <- ParallelLogger::clusterApply(cluster, outcomeIdGroups, exposureNullDistsProc, config = config, analysisId = analysisId)
  rows <- do.call(rbind, res)
  colnames(rows) <- SqlRender::camelCaseToSnakeCase(colnames(rows))
  pgCopyDataFrame(config$connectionDetails, rows, schema = config$rewardbResultsSchema, "exposure_null_distributions")
}