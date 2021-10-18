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
         is_excluded,
         include_descendants
  FROM @schema.atlas_exposure_concept
  "
  exposureCohortConceptSets <- model$queryDb(sql, snakeCaseToCamelCase = TRUE)

  getControlConcepts <- function(conceptSet) {
    if (nrow(conceptSet) == 0)
      return(data.frame())

    return(cemConnection$getSuggestedControlCondtions(conceptSet))
  }

  # Get mapped negative control evidence, keeps targetCohortId
  negativControlConcepts <- exposureCohortConceptSets %>%
    dplyr::group_by(targetCohortId) %>%
    dplyr::filter(!all(as.logical(isExcluded))) %>% # Exclude groupings that only have excluded elements in concept set
    dplyr::group_modify(~getControlConcepts(.x), .keep = TRUE)

  if (nrow(negativControlConcepts)) {
    negativControlConcepts <- negativControlConcepts %>%
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
  } else {
    warning("No concept mappings found")
  }
}

#' Gets negative control outcome concepts for exposure cohorts
getCemOutcomeNegativeControlConcepts <- function(globalConfig) {
  cemConnection <- getCemConnection()
  on.exit(cemConnection$connection$closeConnection(), add = TRUE)
  globalConfig$useConnectionPool <- FALSE
  model <- ReportDbModel(globalConfig)
  sql <- "
  SELECT DISTINCT
    ocd.cohort_definition_id as outcome_cohort_id,
    c.concept_id,
    c.INCLUDE_DESCENDANTS,
    c.IS_EXCLUDED
  from @schema.concept_set_definition c
  INNER JOIN @schema.outcome_cohort_definition ocd ON ocd.CONCEPTSET_ID = c.conceptset_id
  INNER JOIN @schema.scc_result r ON r.outcome_cohort_id = ocd.cohort_definition_id
  WHERE rr IS NOT NULL

  UNION

  SELECT cohort_definition_id as outcome_cohort_id,
         concept_id,
         include_descendants,
         is_excluded
  FROM @schema.atlas_outcome_concept
  "
  outcomeCohortConceptSets <- model$queryDb(sql, snakeCaseToCamelCase = TRUE)

  getControlConcepts <- function(conceptSet) {
    if (nrow(conceptSet) == 0)
      return(data.frame())

    return(cemConnection$getSuggestedControlIngredients(conceptSet))
  }

  # Get mapped negative control evidence, keeps targetCohortId
  negativControlConcepts <- outcomeCohortConceptSets %>%
    dplyr::group_by(outcomeCohortId) %>%
    dplyr::filter(!all(as.logical(isExcluded))) %>% # Exclude groupings that only have excluded elements in concept set
    dplyr::group_modify(~getControlConcepts(.x), .keep = TRUE)

  if (nrow(negativControlConcepts)) {
    negativControlConcepts <- negativControlConcepts %>%
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
  } else {
    warning("No concept mappings found")
  }
}


#' @title
#' meta analysis
#' @description
#' compute meta-analysis across data sources provided in table
#' Perform meta-analysis on data sources
#' @param table data.frame
#' @return data.frame - results of meta analysis
#' @importFrom meta metainc
runMetaAnalysis <- function(table) {
  # Compute meta analysis with random effects model
  results <- meta::metainc(data = table,
                           event.e = tCases,
                           time.e = tPt,
                           event.c = cCases,
                           time.c = cPt,
                           sm = "IRR",
                           model.glmm = "UM.RS")

  row <- data.frame(sourceId = -99,
                    outcomeCohortId = table$outcomeCohortId[1],
                    targetCohortId = table$targetCohortId[1],
                    analysisId = table$analysisId[1],
                    tAtRisk = sum(table$tAtRisk),
                    tPt = sum(table$tPt),
                    tCases = sum(table$tCases),
                    cAtRisk = sum(table$cAtRisk),
                    cPt = sum(table$cPt),
                    cCases = sum(table$cCases),
                    rr = exp(results$TE.random),
                    seLogRr = results$seTE.random,
                    lb95 = exp(results$lower.random),
                    ub95 = exp(results$upper.random),
                    pValue = results$pval.random,
                    i2 = results$I2)

  if ("outcomeType" %in% names(table)) {
    row$outcomeType <- table$outcomeType[1]
  }

  return(row)
}

populateOutcomeCohortNullData <- function(config, analysisIds, sourceIds = NULL, minCohortSize = 5, nThreads = 10) {
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
                                analysis_id = analysisIds,
                                source_ids = sourceIds)

  # compute meta-analysis
  sql <- "SELECT * FROM @results_schema.outcome_cohort_null_data;"
  nullData <- DatabaseConnector::renderTranslateQuerySql(connection,
                                                         sql,
                                                         results_schema = config$rewardbResultsSchema,
                                                         snakeCaseToCamelCase = TRUE)

  if (nrow(nullData) > 0) {
    # Cut in to group by exposure id, outcome_id, analysis id
    groupedNulls <- nullData %>% dplyr::group_split(analysisId, outcomeCohortId, targetCohortId)
  
    res <- ParallelLogger::clusterApply(cluster, groupedNulls, runMetaAnalysis)
    rows <- do.call(rbind, res)

    colnames(rows) <- SqlRender::camelCaseToSnakeCase(colnames(rows))
    rows <- dplyr::rename(rows, i2 = i_2)

    DatabaseConnector::insertTable(connection,
                                   data = rows,
                                   databaseSchema = config$rewardbResultsSchema,
                                   tableName = "outcome_cohort_null_data",
                                   createTable = FALSE,
                                   dropTableIfExists = FALSE)
  }
}

populateExposureCohortNullData <- function(config, analysisIds, sourceIds = NULL, minCohortSize = 5, nThreads = 10) {
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
                                analysis_id = analysisIds,
                                source_ids = sourceIds)

  sql <- "SELECT * FROM @results_schema.exposure_cohort_null_data;"
  nullData <- DatabaseConnector::renderTranslateQuerySql(connection,
                                                         sql,
                                                         results_schema = config$rewardbResultsSchema,
                                                         snakeCaseToCamelCase = TRUE)

  if (nrow(nullData) > 0) {
    # Cut in to group by exposure id, source id, analysis id
    nullData <- nullData %>% dplyr::group_split(analysisId, targetCohortId, outcomeCohortId)
    res <- ParallelLogger::clusterApply(cluster, nullData, runMetaAnalysis)
    rows <- do.call(rbind, res)
    colnames(rows) <- SqlRender::camelCaseToSnakeCase(colnames(rows))
    rows <- dplyr::rename(rows, i2 = i_2)
    DatabaseConnector::insertTable(connection,
                                   data = rows,
                                   databaseSchema = config$rewardbResultsSchema,
                                   tableName = "exposure_cohort_null_data",
                                   createTable = FALSE,
                                   dropTableIfExists = FALSE)
  }
}

outcomeNullDistsProc <- function(nullData) {
  nullDist <- EmpiricalCalibration::fitNull(logRr = log(nullData$rr), seLogRr = nullData$seLogRr)
  absSysError <- EmpiricalCalibration::computeExpectedAbsoluteSystematicError(nullDist)
  results <- data.frame(sourceId = nullData$sourceId[1],
                        analysisId = nullData$analysisId[1],
                        outcomeType = nullData$outcomeType[1],
                        targetCohortId = nullData$targetCohortId[1],
                        nullDistMean = nullDist["mean"],
                        nullDistSd = nullDist["sd"],
                        absoluteError = absSysError,
                        nControls = nrow(nullData))

  return(results)
}

exposureNullDistsProc <- function(nullData) {
  nullDist <- EmpiricalCalibration::fitNull(logRr = log(nullData$rr), seLogRr = nullData$seLogRr)
  absSysError <- EmpiricalCalibration::computeExpectedAbsoluteSystematicError(nullDist)
  results <- data.frame(sourceId = nullData$sourceId[1],
                        analysisId = nullData$analysisId[1],
                        outcomeCohortId = nullData$outcomeCohortId[1],
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
computeOutcomeNullDistributions <- function(config, analysisId = 1, nThreads = 10) {
  cluster <- ParallelLogger::makeCluster(nThreads)
  connection <- DatabaseConnector::connect(config$connectionDetails)
  on.exit({
    DatabaseConnector::disconnect(connection)
    ParallelLogger::stopCluster(cluster)
  }, add = TRUE)

  sql <- "SELECT * FROM @results_schema.outcome_cohort_null_data;"
  nullData <- DatabaseConnector::renderTranslateQuerySql(connection,
                                                         sql,
                                                         results_schema = config$rewardbResultsSchema,
                                                         snakeCaseToCamelCase = TRUE)
  # Cut in to group by exposure id, source id, analysis id
  nullData <- nullData %>% dplyr::group_split(sourceId, analysisId, targetCohortId, outcomeType)
  res <- ParallelLogger::clusterApply(cluster, nullData, outcomeNullDistsProc)

  if (length(res) > 0) {
    rows <- do.call(rbind, res)
    colnames(rows) <- SqlRender::camelCaseToSnakeCase(colnames(rows))
    pgCopyDataFrame(config$connectionDetails, rows, schema = config$rewardbResultsSchema, "outcome_null_distributions")
  }
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

  sql <- "SELECT * FROM @results_schema.exposure_cohort_null_data;"
  nullData <- DatabaseConnector::renderTranslateQuerySql(connection,
                                                         sql,
                                                         results_schema = config$rewardbResultsSchema,
                                                         snakeCaseToCamelCase = TRUE)
  # Cut in to group by exposure id, source id, analysis id
  nullData <- nullData %>% dplyr::group_split(sourceId, analysisId, outcomeCohortId)
  res <- ParallelLogger::clusterApply(cluster, nullData, exposureNullDistsProc)
  if (length(res) > 0) {
    rows <- do.call(rbind, res)
    colnames(rows) <- SqlRender::camelCaseToSnakeCase(colnames(rows))
    pgCopyDataFrame(config$connectionDetails, rows, schema = config$rewardbResultsSchema, "exposure_null_distributions")
  }
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
                                           analysisId = c(1:5),
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

  message("Populating outcome cohort null data")
  populateOutcomeCohortNullData(globalConfig, analysisId, sourceIds = sourceIds, minCohortSize = minCohortSize)

  message("Populating exposure cohort null data")
  populateExposureCohortNullData(globalConfig, analysisId, sourceIds = sourceIds, minCohortSize = minCohortSize)

  message("Computing Expsoure null distributions")
  computeExposureNullDistributions(globalConfig,
                                   analysisId = analysisId,
                                   nThreads = nThreads)
  message("Computing Outcome null distributions")
  computeOutcomeNullDistributions(globalConfig,
                                  analysisId = analysisId,
                                  nThreads = nThreads)
}


#' @title
#' Run pre compute null distributions Rstudio Job
#' @description
#' Run task to pre-compute null distributions for all outcomes and exposures as RStudio Job
#' This process requires a CemConnector connection, this may be slow if using cem.ohdsi.org
#'
#' @inheritParams runPreComputeNullDistributions
#' @param workingDir                        Charachter working directory (default is currentWd)
#' @export
runPreComputeNullDistributionsJob <- function(globalConfigPath,
                                              analysisId = 1,
                                              sourceIds = NULL,
                                              nThreads = 10,
                                              getCemMappings = TRUE,
                                              minCohortSize = 5,
                                              workingDir = getwd()) {
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