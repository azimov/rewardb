createCohorts <- function(connection, config, dataSources) {
  for (ds in dataSources) {
    dataSource <- config$dataSources[[ds]]
    sql <- SqlRender::readSql(system.file("sql/cohorts", "createCohortTable.sql", package = "rewardb"))
    DatabaseConnector::renderTranslateExecuteSql(
      connection,
      sql = sql,
      cohort_database_schema = config$cdmDatabase$schema,
      cohort_table = dataSource$cohortTable
    )

    sql <- SqlRender::readSql(system.file("sql/cohorts", "createCohorts.sql", package = "rewardb"))
    options <- list(
      connection=connection,
      sql = sql,
      cdm_database_schema = dataSource$cdmDatabaseSchema,
      drug_era_schema = dataSource$cdmDatabaseSchema, # Use cdm drug eras
      cohort_database_schema = config$cdmDatabase$schema,
      cohort_definition_table = config$cdmDatabase$cohortDefinitionTable,
      conceptset_definition_table = config$cdmDatabase$conceptSetDefinitionTable,
      vocab_schema = config$cdmDatabase$vocabularySchema,
      cohort_table = dataSource$cohortTable
    )
    do.call(DatabaseConnector::renderTranslateExecuteSql, options)
    # Custom drug eras
    options$drug_era_schema <- dataSource$drugEraSchema # Custom drug era tables live here
    do.call(DatabaseConnector::renderTranslateExecuteSql, options)
  }
}

createOutcomeCohorts <- function(connection, config, dataSources) {
  sql <- SqlRender::readSql(system.file("sql/cohorts", "createOutcomeCohorts.sql", package = "rewardb"))
  for (ds in dataSources) {
    dataSource <- config$dataSources[[ds]]
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

addAtlasOutcomeCohort <- function (connection, config, atlasId, dataSources) {
    cohortDefinition <- ROhdsiWebApi::getCohortDefinition(atlasId, config$webApiUrl)
    sql <- ROhdsiWebApi::getCohortSql(cohortDefinition, config$webApiUrl, generateStats = FALSE)

    ParallelLogger::logInfo(paste("Generating cohort",  atlasId, "from ATLAS SQL definition"))
    for (ds in dataSources) {
      dataSource <- config$dataSources[[ds]]
      DatabaseConnector::renderTranslateExecuteSql(
        connection,
        sql = sql,
        cdm_database_schema = dataSource$cdmDatabaseSchema,
        vocabulary_database_schema = config$cdmDatabase$vocabularySchema,
        target_database_schema = config$cdmDatabase$schema,
        target_cohort_table = dataSource$outcomeCohortTable,
        target_cohort_id = atlasId
      )
    }
}

createCustomDrugEras <- function (connection, config) {
    sql <- SqlRender::readSql(system.file("sql/create", "customDrugEra.sql", package = "rewardb"))
    for (dataSource in config$dataSources) {
      DatabaseConnector::renderTranslateExecuteSql(
        connection,
        sql = sql,
        cdm_database = dataSource$database
      )
    }
}