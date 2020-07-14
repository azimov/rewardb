createCohorts <- function(connection, config) {

  base::writeLines("Creating exposure cohorts...")
  for (dataSource in config$dataSources) {
    sql <- SqlRender::readSql(system.file("sql/cohorts", "createCohortTable.sql", package = "rewardb"))
    DatabaseConnector::renderTranslateExecuteSql(
      connection,
      sql = sql,
      cohort_database_schema = cohort_database_schema = config$cdmDatabase$schema,
      cohort_table = dataSource$cohortTable
    )

    sql <- SqlRender::readSql(system.file("sql/cohorts", "createCohorts.sql", package = "rewardb"))
    DatabaseConnector::renderTranslateExecuteSql(
      connection,
      sql = sql,
      cdm_database_schema = dataSource$cdmDatabaseSchema,
      drug_era_schema = dataSource$drugEraSchema,
      cohort_database_schema = config$cdmDatabase$schema,
      cohort_definition_table = config$cdmDatabase$cohortDefinitionTable,
      conceptset_definition_table = config$cdmDatabase$conceptSetDefinitionTable,
      vocab_schema = config$cdmDatabase$vocabularySchema,
      cohort_table = dataSource$cohortTable
    )
  }
}

createOutcomeCohorts <- function(connection, config) {
  base::writeLines("Creating outcome cohorts...")

  for (dataSource in config$dataSources) {
    sql <- SqlRender::readSql(system.file("sql/cohorts", "createOutcomeCohortTable.sql", package = "rewardb"))
    DatabaseConnector::renderTranslateExecuteSql(
      connection,
      sql = sql,
      cohort_database_schema = config$cdmDatabase$schema,
      outcome_cohort_table = dataSource$outcomeCohortTable
    )
    sql <- SqlRender::readSql(system.file("sql/cohorts", "createOutcomeCohorts.sql", package = "rewardb"))

    DatabaseConnector::renderTranslateExecuteSql(
      connection,
      sql = sql,
      cdm_database_schema = dataSource$cdmDatabaseSchema,
      cohort_database_schema = config$cdmDatabase$schema,
      outcome_cohort_table = dataSource$outcomeCohortTable,
      cdm_outcome_cohort_schema = dataSource$cdmOutcomeCohortSchema,
      cdm_outcome_cohort_table = dataSource$cdmOutcomeCohortTable,
      custom_outcome_cohort_list = customOutcomeCohortList
    )
  }
}

# TODO: This requires the cohorts to have been generated in atlas, check if there is a way to see if this has happened in web API
addCustomOutcomes <- function (connection, config, atlasId) {
    sql <- SqlRender::readSql(system.file("sql/create", "customAtlasCohorts.sql", package = "rewardb"))
    for (dataSource in config$dataSources) {
      DatabaseConnector::renderTranslateExecuteSql(
        connection,
        sql = sql,
        cdm_outcome_cohort_schema = dataSource$cdmOutcomeCohortSchema,
        cohort_database_schema = config$cdmDatabase$schema,
        outcome_cohort_table = dataSource$outcomeCohortTable
      )
    }
}