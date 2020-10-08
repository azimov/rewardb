#' Names for target cohorts
#' @param connection DatabaseConnector connection to cdm
#' @param config
createTargetDefinitions <- function(connection, config) {
  sql <- SqlRender::readSql(system.file("sql/cohorts", "createIngredientConceptReferences.sql", package = "rewardb"))
  ingredients <- DatabaseConnector::renderTranslateExecuteSql(
    connection,
    sql,
    vocabulary_database_schema = config$cdmDatabase$vocabularySchema,
    cohort_database_schema = config$cdmDatabase$schema,
    concept_set_definition_table = config$cdmDatabase$conceptSetDefinitionTable,
    cohort_definition_table = config$cdmDatabase$cohortDefinitionTable
  )
}

#' Create reference tables used to compute things in rewardb
#' TODO: split up data sources and main reference tables in to different function calls
#' @param connection DatabaseConnector connection to cdm
#' @param config
#' @param dataSources dataSources to run cohort on
createReferenceTables <- function(connection, config, dataSources) {
  ParallelLogger::logInfo("Removing and inserting references")
  sql <- SqlRender::readSql(system.file("sql/create", "createReferenceTables.sql", package = "rewardb"))
  DatabaseConnector::renderTranslateExecuteSql(
    connection,
    sql = sql,
    cohort_database_schema = config$cdmDatabase$schema,
    conceptset_definition_table = config$cdmDatabase$conceptSetDefinitionTable,
    cohort_definition_table = config$cdmDatabase$cohortDefinitionTable,
    outcome_cohort_definition_table = config$cdmDatabase$outcomeCohortDefinitionTable
  )

  sql <- SqlRender::readSql(system.file("sql/create", "atlasCohortReferenceTables.sql", package = "rewardb"))
  DatabaseConnector::renderTranslateExecuteSql(
    connection,
    sql = sql,
    cohort_database_schema = config$cdmDatabase$schema,
    atlas_outcome_reference = config$cdmDatabase$atlasCohortReferenceTable,
    atlas_concept_reference = config$cdmDatabase$atlasConceptReferenceTable
  )

  ParallelLogger::logInfo("Inserting ingredient/ATC cohorts")
  createTargetDefinitions(connection, config)

  sql <- SqlRender::readSql(system.file("sql/create", "outcomeCohortDefinitions.sql", package = "rewardb"))

  for (dataSource in dataSources) {
    DatabaseConnector::renderTranslateExecuteSql(
      connection,
      sql = sql,
      dbms = connection@dbms,
      cdm_database_schema = dataSource$cdmDatabaseSchema,
      cohort_database_schema = config$cdmDatabase$schema,
      outcome_cohort_definition_table = config$cdmDatabase$outcomeCohortDefinitionTable
    )
  }

  sql <- SqlRender::readSql(system.file("sql/cohorts", "createOutcomeCohortTable.sql", package = "rewardb"))
  for (dataSource in dataSources) {
    DatabaseConnector::renderTranslateExecuteSql(
      connection,
      sql = sql,
      cohort_database_schema = config$cdmDatabase$schema,
      outcome_cohort_table = dataSource$outcomeCohortTable
    )
  }
}