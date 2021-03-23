#' Load, render, and translate a SQL file in this package.
#'
#' @description
#' This helper function is used in place of
#' using \code{SqlRender::loadRenderTranslateSql}
#' otherwise unit tests will not function properly.
#'
#' NOTE: This function does not support dialect-specific SQL translation
#' at this time.
#'
#' @param sqlFilename               The source SQL file
#' @param dbms                      The target dialect. Currently 'sql server', 'oracle', 'postgres',
#'                                  and 'redshift' are supported
#' @param ...                       Parameter values used for \code{render}
#' @param tempEmulationSchema       Some database platforms like Oracle and Impala do not truly support
#'                                  temp tables. To emulate temp tables, provide a schema with write
#'                                  privileges where temp tables can be created.
#' @param warnOnMissingParameters   Should a warning be raised when parameters provided to this
#'                                  function do not appear in the parameterized SQL that is being
#'                                  rendered? By default, this is TRUE.
#'
#' @return
#' Returns a string containing the rendered SQL.
loadRenderTranslateSql <- function(sqlFilename,
                                   dbms = "sql server",
                                   ...,
                                   tempEmulationSchema = getOption("sqlRenderTempEmulationSchema"),
                                   warnOnMissingParameters = TRUE) {
  pathToSql <- system.file(paste("sql/sql_server"),
                           sqlFilename,
                           package = "rewardb",
                           mustWork = TRUE)
  sql <- SqlRender::readSql(pathToSql)
  renderedSql <- SqlRender::render(sql = sql,
                                   warnOnMissingParameters = warnOnMissingParameters,
                                   ...)
  renderedSql <- SqlRender::translate(sql = renderedSql,
                                      targetDialect = dbms,
                                      tempEmulationSchema = tempEmulationSchema)
  return(renderedSql)
}

#' From pairs of exposure, outcomes get dataframe of assocaited RR values from reward data
#' @description
#' Function to get cohorts from outcome/exposure pairs. Will get any cohorts results for cohorts
#' that match these pairs including any ancestor concepts.
#'
#' @param connection                DatabaseConnector connection object
#' @param config                    Reward global config object
#' @param outcomeExposurePairs      Dataframe containing the coulmns 'exposureConceptId' and 'outcomeConceptId'
#' @param snakeCaseToCamelCase      convert snake case to camle case in output frame
#' @return
#' Dataframe containing RR results for cohorts that match the concept id pairs
getConcetptCohortData <- function(connection, config, outcomeExposurePairs, snakeCaseToCamelCase = TRUE, outcomeTypes = 0) {
  exposureConceptIds <- unique(outcomeExposurePairs$exposureConceptId)
  outcomeConceptIds <- unique(outcomeExposurePairs$outcomeConceptId)

  DatabaseConnector::insertTable(connection,
                                 tableName = "o_exp_pair",
                                 data = outcomeExposurePairs,
                                 camelCaseToSnakeCase = TRUE,
                                 tempTable = TRUE,
                                 bulkLoad  = TRUE)

  sql <- loadRenderTranslateSql("getRrFromConceptIds.sql",
                                results_schema = config$rewardbResultsSchema,
                                reference_schema = config$rewardbResultsSchema,
                                exposure_concept_ids = exposureConceptIds,
                                outcome_concept_ids = outcomeConceptIds,
                                outcome_types = outcomeTypes)

  data <- DatabaseConnector::querySql(connection, sql, snakeCaseToCamelCase = snakeCaseToCamelCase)
}

#' From pairs of exposure, outcomes get dataframe of assocaited RR values from reward data
#' @description
#' Function to get cohorts from outcome/exposure pairs. Will get any cohorts results for cohorts
#' that match these pairs including any ancestor concepts.
#'
#' @param connection                DatabaseConnector connection object
#' @param config                    Reward global config object
#' @param outcomeExposurePairs      Dataframe containing the coulmns 'exposureConceptId' and 'outcomeConceptId'
#' @param snakeCaseToCamelCase      convert snake case to camle case in output frame
#' @return
#' Dataframe containing RR results for cohorts that match the concept id pairs
getConceptCohortDataFromAtlasOutcomes <- function(connection, config, outcomeExposurePairs, snakeCaseToCamelCase = TRUE) {
  exposureConceptIds <- unique(outcomeExposurePairs$exposureConceptId)

  DatabaseConnector::insertTable(connection,
                                 tableName = "o_exp_pair",
                                 data = outcomeExposurePairs,
                                 camelCaseToSnakeCase = TRUE,
                                 tempTable = TRUE,
                                 bulkLoad  = TRUE)

  sql <- loadRenderTranslateSql("getExposureRrFromAtlasOutcomes.sql",
                                results_schema = config$rewardbResultsSchema,
                                reference_schema = config$rewardbResultsSchema,
                                exposure_concept_ids = exposureConceptIds)

  data <- DatabaseConnector::querySql(connection, sql, snakeCaseToCamelCase = snakeCaseToCamelCase)
}

getExposureControlConcepts <- function (connection, config, outcomeConceptIds) {
  sql <- "SELECT ingredient_concept_id as exposure_concept_id, condition_concept_id as outcome_concept_id FROM @cem_schema.matrix_summary
  WHERE evidence_exists = 0 and condition_concept_id IN (@outcome_concept_ids);"

  DatabaseConnector::renderTranslateQuerySql(connection,
                                             sql,
                                             cem_schema = config$cemSchema,
                                             outcome_concept_ids = outcomeConceptIds,
                                             snakeCaseToCamelCase = TRUE)
}

getOutcomeControlConcepts <- function (connection, config, exposureConceptIds) {
  sql <- "SELECT ingredient_concept_id as exposure_concept_id, condition_concept_id as outcome_concept_id FROM @cem_schema.matrix_summary
  WHERE evidence_exists = 0 and ingredient_concept_id IN (@exposure_concept_ids);"

  DatabaseConnector::renderTranslateQuerySql(connection,
                                             sql,
                                             cem_schema = config$cemSchema,
                                             exposure_concept_ids = exposureConceptIds,
                                             snakeCaseToCamelCase = TRUE)
}