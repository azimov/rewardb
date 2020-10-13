#' Create exposures cohorts in the CDM - this function can take a long time
#' @param connection DatabaseConnector connection to cdm
#' @param config
#' @param dataSources dataSources to run cohort on
createCohorts <- function(connection, config) {
  sql <- SqlRender::readSql(system.file("sql/cohorts", "createCohortTable.sql", package = "rewardb"))
  DatabaseConnector::renderTranslateExecuteSql(
    connection,
    sql = sql,
    cohort_database_schema = config$resultSchema,
    cohort_table = config$tables$cohort
  )

  sql <- SqlRender::readSql(system.file("sql/cohorts", "createCohorts.sql", package = "rewardb"))
  options <- list(
    connection=connection,
    sql = sql,
    cdm_database_schema = config$cdmSchema,
    reference_schema = config$referenceSchema,
    drug_era_schema = config$cdmSchema, # Use cdm drug eras
    cohort_database_schema = config$resultSchema,
    vocab_schema = config$vocabularySchema,
    cohort_table = config$tables$cohort
  )
  do.call(DatabaseConnector::renderTranslateExecuteSql, options)
  # Custom drug eras
  if (!is.null(config$drugEraSchema)) {
    options$drug_era_schema <- config$drugEraSchema # Custom drug era tables live here
    do.call(DatabaseConnector::renderTranslateExecuteSql, options)
  }

}

#' Pulls concept set from webApi and adds cohort
#' @param connection DatabaseConnector connection to cdm
#' @param config
#' @param conceptSetId concept set in WebAPI
#' @param dataSources dataSources to run cohort on
addCustomExposureCohort <- function (connection, config, conceptSetIds, dataSource) {
  # Add conceptSetId/name to custom_exposure_cohort table
  # Get all items, add them to the custom_exposure_concept table
   options <- list(
     connection = connection,
     sql = SqlRender::readSql(system.file("sql/cohorts", "addCustomExposureCohorts.sql", package = "rewardb")),
     cdm_database_schema = config$cdmSchema,
     drug_era_schema = config$cdmSchema, # Use cdm drug eras
     cohort_database_schema = config$resultSchema,
     vocab_schema = config$vocabularySchema,
     cohort_table = config$tables$cohort,
     only_add_subset = 1,
     custom_exposure_subset = conceptSetIds
   )
   do.call(DatabaseConnector::renderTranslateExecuteSql, options)
   # Custom drug eras
   options$drug_era_schema <- config$drugEraSchema # Custom drug era tables live here
   do.call(DatabaseConnector::renderTranslateExecuteSql, options)
}

#' Create outcome cohorts in the CDM - this function can take a very very long time
#' @param connection DatabaseConnector connection to cdm
#' @param config
#' @param dataSources dataSources to run cohort on
createOutcomeCohorts <- function(connection, config) {
  sql <- SqlRender::readSql(system.file("sql/cohorts", "createOutcomeCohorts.sql", package = "rewardb"))
  DatabaseConnector::renderTranslateExecuteSql(
      connection,
      sql = sql,
      reference_schema = config$referenceSchema,
      cdm_database_schema = config$cdmSchema,
      cohort_database_schema = config$resultSchema,
      outcome_cohort_table = config$tables$outcomeCohort
  )
}

#' create the custom drug eras, these are for drugs with nonstandard eras (e.g. where doeses aren't picked up by
#' repeat perscriptions). Could be something like a vaccine where exposed time is non trivial.
#' @param connection DatabaseConnector connection to cdm
#' @param
createCustomDrugEras <- function (connection, config) {
    sql <- SqlRender::readSql(system.file("sql/create", "customDrugEra.sql", package = "rewardb"))
    DatabaseConnector::renderTranslateExecuteSql(
      connection,
      sql = sql,
      cdm_database = config$cdmSchema
    )

}