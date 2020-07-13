createSummaryTables <- function (connection, config) {
  sql <- SqlRender::readSql(system.file("sql/cohorts", "createOutcomeSummary.sql", package = "rewardb"))
  for (dataSource in config$dataSources) {
    DatabaseConnector::renderTranslateExecuteSql(
      connection,
      sql = sql,
      cohort_database_schema = config$cdmDatabase$schema,
      cdm_outcome_cohort_schema = dataSource$cdmOutcomeCohortSchema,
      cohort_database_schema = config$cdmDatabase$schema,
      outcome_cohort_table = dataSource$outcomeCohortTable,
      outcome_cohort_definition_table = config$cdmDatabase$outcomeCohortDefinitionTable,
      outcome_summary_table = dataSource$outcomeSummaryTable,
      cohort_table = dataSource$cohortTable
    )
  }
}