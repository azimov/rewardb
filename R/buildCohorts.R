createCohorts <- function(connection, config) {
  for (dataSource in config$dataSources) {
    sql <- SqlRender::readSql(system.file("sql/cohorts", "createCohortTable.sql", package = "rewardb"))
    DatabaseConnector::renderTranslateExecuteSql(
      connection,
      sql = sql,
      cohort_database_schema = config$cdmDatabase$schema,
      cohort_table = dataSource$cohortTable
    )

    sql <- SqlRender::readSql(system.file("sql/cohorts", "createCohorts.sql", package = "rewardb"))
    DatabaseConnector::renderTranslateExecuteSql(
      connection,
      sql = sql,
      cdm_database_schema = dataSource$cdmDatabaseSchema,
      drug_era_schema = dataSource$cdmDatabaseSchema, # Use cdm drug eras
      cohort_database_schema = config$cdmDatabase$schema,
      cohort_definition_table = config$cdmDatabase$cohortDefinitionTable,
      conceptset_definition_table = config$cdmDatabase$conceptSetDefinitionTable,
      vocab_schema = config$cdmDatabase$vocabularySchema,
      cohort_table = dataSource$cohortTable
    )

    # Custom drug eras
    DatabaseConnector::renderTranslateExecuteSql(
      connection,
      sql = sql,
      cdm_database_schema = dataSource$cdmDatabaseSchema,
      drug_era_schema = dataSource$drugEraSchema, # Custom drug era tables live here
      cohort_database_schema = config$cdmDatabase$schema,
      cohort_definition_table = config$cdmDatabase$cohortDefinitionTable,
      conceptset_definition_table = config$cdmDatabase$conceptSetDefinitionTable,
      vocab_schema = config$cdmDatabase$vocabularySchema,
      cohort_table = dataSource$cohortTable
    )
  }
}

createOutcomeCohorts <- function(connection, config) {
  sql <- SqlRender::readSql(system.file("sql/cohorts", "createOutcomeCohorts.sql", package = "rewardb"))
  for (dataSource in config$dataSources) {
    DatabaseConnector::renderTranslateExecuteSql(
      connection,
      sql = sql,
      cdm_database_schema = dataSource$cdmDatabaseSchema,
      cohort_database_schema = config$cdmDatabase$schema,
      outcome_cohort_table = dataSource$outcomeCohortTable,
      cdm_outcome_cohort_schema = dataSource$cdmOutcomeCohortSchema,
      cdm_outcome_cohort_table = dataSource$cdmOutcomeCohortTable
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

createCustomDrugEras <- function (connection, config, atlasId) {
    sql <- SqlRender::readSql(system.file("sql/create", "customDrugEra.sql", package = "rewardb"))
    for (dataSource in config$dataSources) {
      DatabaseConnector::renderTranslateExecuteSql(
        connection,
        sql = sql,
        cdm_database = dataSource$database
      )
    }
}