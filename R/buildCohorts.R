#' Create exposures cohorts in the CDM - this function can take a long time
#' @param connection DatabaseConnector connection to cdm
#' @param config
#' @param dataSources dataSources to run cohort on
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

#' Create outcome cohorts in the CDM - this function can take a very very long time
#' @param connection DatabaseConnector connection to cdm
#' @param config
#' @param dataSources dataSources to run cohort on
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

#' Pulls cohort from atlas and then creates it in the rewardb outcome cohorts table
#' @param connection DatabaseConnector connection to cdm
#' @param config
#' @param atlasId atlasId in WebAPI
#' @param dataSources dataSources to run cohort on
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

#' create the custom drug eras, these are for drugs with nonstandard eras (e.g. where doeses aren't picked up by
#' repeat perscriptions). Could be something like a vaccine where exposed time is non trivial.
#' @param connection DatabaseConnector connection to cdm
#' @param
createCustomDrugEras <- function (connection, config, dataSources) {
    sql <- SqlRender::readSql(system.file("sql/create", "customDrugEra.sql", package = "rewardb"))
    for (ds in dataSources) {
      dataSource <- config$dataSources[[ds]]
      DatabaseConnector::renderTranslateExecuteSql(
        connection,
        sql = sql,
        cdm_database = dataSource$database
      )
    }
}