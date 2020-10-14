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
    connection = connection,
    sql = sql,
    cdm_database_schema = config$cdmSchema,
    reference_schema = config$referenceSchema,
    drug_era_schema = config$cdmSchema, # Use cdm drug eras
    cohort_database_schema = config$resultSchema,
    vocab_schema = config$vocabularySchema,
    cohort_table = config$tables$cohort
  )

  customExposureOptions <- list(
    connection = connection,
    sql = SqlRender::readSql(system.file("sql/cohorts", "addCustomExposureCohorts.sql", package = "rewardb")),
    cdm_database_schema = config$cdmSchema,
    reference_schema = config$referenceSchema,
    drug_era_schema = config$cdmSchema, # Use cdm drug eras
    cohort_database_schema = config$resultSchema,
    vocab_schema = config$vocabularySchema,
    cohort_table = config$tables$cohort,
    only_add_subset = 0
  )
  do.call(DatabaseConnector::renderTranslateExecuteSql, options)
  do.call(DatabaseConnector::renderTranslateExecuteSql, customExposureOptions)
  # Custom drug eras
  if (!is.null(config$drugEraSchema)) {
    options$drug_era_schema <- config$drugEraSchema # Custom drug era tables live here
    customExposureOptions$drug_era_schema <- config$drugEraSchema
    do.call(DatabaseConnector::renderTranslateExecuteSql, options)
    do.call(DatabaseConnector::renderTranslateExecuteSql, customExposureOptions)
  }

}


getUncomputedAtlasCohorts <- function(connection, config) {
  # Get only null atlas cohorts
  atlaSql <- "
  SELECT aor.*
  FROM @reference_schema.atlas_outcome_reference aor
  LEFT JOIN
    (
      SELECT DISTINCT cohort_definition_id
      FROM @result_schema.@outcome_cohort_table
    ) oct on oct.cohort_definition_id = aor.cohort_definition_id
  WHERE oct.cohort_definition_id IS NULL
  "
  atlasCohorts <- DatabaseConnector::renderTranslateQuerySql(
    connection,
    atlaSql,
    reference_schema = config$referenceSchema,
    result_schema = config$resultSchema,
    outcome_cohort_table = config$tables$outcomeCohort
  )
  return(atlasCohorts)
}

#' Pulls concept set from webApi and adds cohort
#' @param connection DatabaseConnector connection to cdm
#' @param config
#' @param conceptSetId concept set in WebAPI
#' @param dataSources dataSources to run cohort on
addCustomExposureCohorts <- function(connection, config, conceptSetIds) {
  # Add conceptSetId/name to custom_exposure_cohort table
  # Get all items, add them to the custom_exposure_concept table
  options <- list(
    connection = connection,
    sql = SqlRender::readSql(system.file("sql/cohorts", "addCustomExposureCohorts.sql", package = "rewardb")),
    cdm_database_schema = config$cdmSchema,
    reference_schema = config$referenceSchema,
    drug_era_schema = config$cdmSchema, # Use cdm drug eras
    cohort_database_schema = config$resultSchema,
    vocab_schema = config$vocabularySchema,
    cohort_table = config$tables$cohort,
    only_add_subset = 1,
    custom_exposure_subset = conceptSetIds
  )
  do.call(DatabaseConnector::renderTranslateExecuteSql, options)

  if (!is.null(config$drugEraSchema)) {
    options$drug_era_schema <- config$drugEraSchema # Custom drug era tables live here
    do.call(DatabaseConnector::renderTranslateExecuteSql, options)
  }
}

#' Create outcome cohorts in the CDM - this function can take a very very long time
#' @param connection DatabaseConnector connection to cdm
#' @param config
#' @param dataSources dataSources to run cohort on
createOutcomeCohorts <- function(connection, config) {

  sql <- SqlRender::readSql(system.file("sql/cohorts", "createOutcomeCohortTable.sql", package = "rewardb"))
  DatabaseConnector::renderTranslateExecuteSql(
    connection,
    sql = sql,
    cohort_database_schema = config$resultSchema,
    outcome_cohort_table = config$tables$outcomeCohort
  )

  sql <- SqlRender::readSql(system.file("sql/cohorts", "createOutcomeCohorts.sql", package = "rewardb"))
  DatabaseConnector::renderTranslateExecuteSql(
    connection,
    sql = sql,
    reference_schema = config$referenceSchema,
    cdm_database_schema = config$cdmSchema,
    cohort_database_schema = config$resultSchema,
    outcome_cohort_table = config$tables$outcomeCohort
  )

  atlasCohorts <- getUncomputedAtlasCohorts(connection, config)

  # Generate each cohort
  apply(atlasCohorts, 1, function(cohortReference) {
    DatabaseConnector::renderTranslateExecuteSql(
      connection,
      sql = cohortReference["SQL_DEFINITION"],
      cdm_database_schema = config$cdmSchema,
      vocabulary_database_schema = config$vocabularySchema,
      target_database_schema = config$resultSchema,
      target_cohort_table = config$tables$outcomeCohort,
      target_cohort_id = cohortReference["COHORT_DEFINITION_ID"]
    )
  })

}

#' create the custom drug eras, these are for drugs with nonstandard eras (e.g. where doeses aren't picked up by
#' repeat perscriptions). Could be something like a vaccine where exposed time is non trivial.
#' @param connection DatabaseConnector connection to cdm
#' @param
createCustomDrugEras <- function(connection, config) {
  sql <- SqlRender::readSql(system.file("sql/create", "customDrugEra.sql", package = "rewardb"))
  DatabaseConnector::renderTranslateExecuteSql(
    connection,
    sql = sql,
    cdm_database = config$cdmSchema
  )

}