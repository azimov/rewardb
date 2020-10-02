#' Create outcome summary tables
#' @param connection DatabaseConnector connection object
#' @param config
#' @param dataSources
createSummaryTables <- function (connection, config, dataSources) {
  sql <- SqlRender::readSql(system.file("sql/cohorts", "insertOutcomeSummary.sql", package = "rewardb"))
  createSql <- SqlRender::readSql(system.file("sql/cohorts", "createOutcomeSummary.sql", package = "rewardb"))
  for (dataSource in dataSources) {
    DatabaseConnector::renderTranslateExecuteSql(
      connection,
      sql = createSql,
      cohort_database_schema = config$cdmDatabase$schema,
      outcome_summary_table = dataSource$outcomeSummaryTable,
    )

    DatabaseConnector::renderTranslateExecuteSql(
      connection,
      sql = sql,
      cohort_database_schema = config$cdmDatabase$schema,
      outcome_cohort_table = dataSource$outcomeCohortTable,
      outcome_cohort_definition_table = config$cdmDatabase$outcomeCohortDefinitionTable,
      outcome_summary_table = dataSource$outcomeSummaryTable,
      cdm_database_schema = dataSource$cdmDatabaseSchema,
      cohort_table = dataSource$cohortTable,
      use_custom_outcome_cohort_ids = 0
    )
  }
}

#' Add outcome summary resultS
#' @param connection DatabaseConnector connection object
#' @param config
#' @param outcomeCohortIds
#' @param dataSources
addOutcomeSummary <- function (connection, config, outcomeCohortIds, dataSources) {
  sql <- SqlRender::readSql(system.file("sql/cohorts", "insertOutcomeSummary.sql", package = "rewardb"))
  deleteSql <- "DELETE FROM @cohort_database_schema.@outcome_summary_table WHERE outcome_cohort_definition_id IN (@custom_outcome_cohort_ids)"
  for (dataSource in dataSources) {
    # Remove any existing entries first to prevent duplication of results
    DatabaseConnector::renderTranslateExecuteSql(
      connection,
      sql = deleteSql,
      cohort_database_schema = config$cdmDatabase$schema,
      outcome_summary_table = dataSource$outcomeSummaryTable,
      custom_outcome_cohort_ids = outcomeCohortIds,
    )

    DatabaseConnector::renderTranslateExecuteSql(
      connection,
      sql = sql,
      cohort_database_schema = config$cdmDatabase$schema,
      outcome_cohort_table = dataSource$outcomeCohortTable,
      outcome_cohort_definition_table = config$cdmDatabase$outcomeCohortDefinitionTable,
      outcome_summary_table = dataSource$outcomeSummaryTable,
      cohort_table = dataSource$cohortTable,
      cdm_database_schema = dataSource$cdmDatabaseSchema,
      use_custom_outcome_cohort_ids = 1,
      custom_outcome_cohort_ids = outcomeCohortIds
    )
  }
}