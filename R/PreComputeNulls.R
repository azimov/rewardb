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
  WHERE rr IS NOT NULL

  UNION

  SELECT cohort_definition_id as target_cohort_id,
         concept_id,
         include_descendants,
         is_excluded
  FROM @schema.atlas_exposure_concept
  "
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

  # Cache results in table to speed up process
  loadRenderTranslateExecuteSql(connection,
                                "calibration/outcomeCohortNullData.sql",
                                results_schema = config$rewardbResultsSchema,
                                min_cohort_size = minCohortSize,
                                analysis_id = analysisId,
                                source_ids = sourceIds,
                                snakeCaseToCamelCase = TRUE)

  sql <- "SELECT * FROM @results_schema.outcome_cohort_null_data;"
  nullData <- DatabaseConnector::renderTranslateQuerySql(connection, sql, results_schema = config$rewardbResultsSchema)
  # Cut in to group by exposure id, source id, analysis id
  nullData <- nullData %>% dplyr::group_split(sourceId, analysisId, targetCohortId, outcomeType)
  res <- ParallelLogger::clusterApply(cluster, nullData, outcomeNullDistsProc)
  rows <- do.call(rbind, res)

  colnames(rows) <- SqlRender::camelCaseToSnakeCase(colnames(rows))
  pgCopyDataFrame(config$connectionDetails, rows, schema = config$rewardbResultsSchema, "outcome_null_distributions")
}


#' Gets negative control outcome concepts for exposure cohorts
getCemOutcomeNegativeControlConcepts <- function(globalConfig) {
  cemConnection <- getCemConnection()
  on.exit(cemConnection$connection$closeConnection(), add = TRUE)
  globalConfig$useConnectionPool <- FALSE
  model <- ReportDbModel(globalConfig)
  sql <- "
  SELECT DISTINCT
    cd.cohort_definition_id as outcome_cohort_id,
    c.concept_id,
    c.IS_EXCLUDED,
    c.INCLUDE_DESCENDANTS
  from @schema.concept_set_definition c
  INNER JOIN @schema.outcome_cohort_definition ocd ON ocd.CONCEPTSET_ID = c.conceptset_id
  INNER JOIN @schema.scc_result r ON r.outcome_cohort_id = cd.cohort_definition_id
  WHERE rr IS NOT NULL

  UNION

  SELECT cohort_definition_id as outcome_cohort_id,
         concept_id,
         include_descendants,
         is_excluded
  FROM @schema.atlas_outcome_concept
  "
  outcomeCohortConceptSets <- model$queryDb(sql, snakeCaseToCamelCase = TRUE)
  # Get mapped negative control evidence, keeps targetCohortId
  negativControlConcepts <- outcomeCohortConceptSets %>%
    dplyr::group_by(outcomeCohortId) %>%
    dplyr::group_modify(~cemConnection$getSuggestedControlIngredients(.x), keep = TRUE) %>%
    dplyr::mutate(cohortDefinitionId = outcomeCohortId) %>%
    dplyr::select(cohortDefinitionId, conceptId)

  connection <- DatabaseConnector::connect(globalConfig$connectionDetails)
  on.exit(DatabaseConnector::disconnect(connection), add = TRUE)
  # Insert negative control concept sets
  DatabaseConnector::insertTable(connection,
                                 data = negativControlConcepts,
                                 databaseSchema = globalConfig$rewardbResultsSchema,
                                 tableName = "outcome_negative_control_concept",
                                 dropTableIfExists = TRUE,
                                 camelCaseToSnakeCase = TRUE,
                                 createTable = TRUE)
}

exposureNullDistsProc <- function(nullData) {
  nullDist <- EmpiricalCalibration::fitNull(logRr = log(nullData$rr), seLogRr = nullData$seLogRr)
  absSysError <- EmpiricalCalibration::computeExpectedAbsoluteSystematicError(nullDist)
  results <- data.frame(sourceId = nullData$sourceId[1],
                        analysisId = nullData$analysisId[1],
                        outcomeType = nullData$outcomeType[1],
                        outcomeCohortId = nullData$outcomeCohortId[1],
                        outcomeConceptId = nullData$outcomeConceptId[1],
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
computeExposureNullDistributions <- function(config, analysisId = 1, sourceIds = NULL, nThreads = 10, minCohortSize = 5) {
  cluster <- ParallelLogger::makeCluster(nThreads)
  connection <- DatabaseConnector::connect(config$connectionDetails)
  on.exit({
    DatabaseConnector::disconnect(connection)
    ParallelLogger::stopCluster(cluster)
  }, add = TRUE)

  # Cache results in table to speed up process
  loadRenderTranslateExecuteSql(connection,
                                "calibration/exposureCohortNullData.sql",
                                results_schema = config$rewardbResultsSchema,
                                min_cohort_size = minCohortSize,
                                analysis_id = analysisId,
                                source_ids = sourceIds,
                                snakeCaseToCamelCase = TRUE)

  sql <- "SELECT * FROM @results_schema.exposure_cohort_null_data;"
  nullData <- DatabaseConnector::renderTranslateQuerySql(connection, sql, results_schema = config$rewardbResultsSchema)
  # Cut in to group by exposure id, source id, analysis id
  nullData <- nullData %>% dplyr::group_split(sourceId, analysisId, outcomeCohortId)
  res <- ParallelLogger::clusterApply(cluster, nullData, exposureNullDistsProc)
  rows <- do.call(rbind, res)

  colnames(rows) <- SqlRender::camelCaseToSnakeCase(colnames(rows))
  pgCopyDataFrame(config$connectionDetails, rows, schema = config$rewardbResultsSchema, "exposure_null_distributions")
}

#' @title
#' Run pre compute null distributions
#' @description
#' Run task to pre-compute null distributions for all outcomes and exposures.
#' This process requires a CemConnector connection, this may be slow if using cem.ohdsi.org
#'
#' @param globalConfigPath          Charachter path to global conifg yaml
#' @param analysisId                Integer analysis settings ID (default is 1)
#' @param sourceIds                 Integer vector of integer sources to run on (default is NULL, all sources)
#' @param nThreads                  Integer number of threads
#' @param minCohortSize             Minimum size of cohort to use as a negative control. Default is 5
#' @export
runPreComputeNullDistributions <- function(globalConfigPath,
                                           analysisId = 1,
                                           sourceIds = NULL,
                                           nThreads = 10,
                                           getCemMappings = TRUE,
                                           minCohortSize = 5) {
  globalConfig <- loadGlobalConfiguration(globalConfigPath)

  if (getCemMappings) {
    message("Adding negative control outcome concepts from Common evidence model:")
    getCemExposureNegativeControlConcepts(globalConfig)
    message("Adding negative control exposure concepts from Common evidence model:")
    getCemOutcomeNegativeControlConcepts(globalConfig)
  }

  computeExposureNullDistributions(globalConfig,
                                   analysisId = analysisId,
                                   sourceIds = sourceIds,
                                   sourceIds = sourceIds,
                                   minCohortSize = minCohortSize,
                                   nThreads = nThreads)

  computeOutcomeNullDistributions(globalConfig,
                                  analysisId = analysisId,
                                  sourceIds = sourceIds,
                                  sourceIds = sourceIds,
                                  minCohortSize = minCohortSize,
                                  nThreads = nThreads)
}


#' @title
#' Run pre compute null distributions Rstudio Job
#' @description
#' Run task to pre-compute null distributions for all outcomes and exposures as RStudio Job
#' This process requires a CemConnector connection, this may be slow if using cem.ohdsi.org
#' @export
runPreComputeNullDistributionsJob <- function(globalConfigPath,
                                              analysisId = 1,
                                              sourceIds = NULL,
                                              nThreads = 10,
                                              getCemMappings = TRUE,
                                              minCohortSize = 5,
                                              workingDir = ".") {
  scriptPath <- system.file("scripts/runPreComputeNulls.R", package = "rewardb")
  .GlobalEnv$analysisId <- analysisId
  .GlobalEnv$globalConfigPath <- normalizePath(globalConfigPath)
  .GlobalEnv$sourceIds <- sourceIds
  .GlobalEnv$nThreads <- nThreads
  .GlobalEnv$getCemMappings <- getCemMappings
  .GlobalEnv$minCohortSize <- minCohortSize
  workingDir <- normalizePath(workingDir)
  rstudioapi::jobRunScript(scriptPath, name = "Pre Compute Null Distributions", workingDir = workingDir, importEnv = TRUE)
}