createTargetDefinitions <- function(connection, config) {
  sql <- SqlRender::readSql(system.file("sql/cohorts", "createIngredientConceptReferences.sql", package = "rewardb"))
  ingredients <- DatabaseConnector::renderTranslateExecuteSql(
    connection,
    sql,
    vocabulary_database_schema = config$cdmDatabase$vocabularySchema,
    cohort_database_schema = config$cdmDatabase$schema,
    concept_set_definition_table = config$cdmDatabase$conceptSetDefinitionTable,
    cohort_definition_table = config$cdmDatabase$conceptSetDefinitionTable
  )
}

createAtlasReference <- function(connection, config, dataSource, customOutcomeCohortList) {

  sql <- SqlRender::readSql(system.file("sql/create", "customAtalsCohorts.sql", package = "rewardb"))
  DatabaseConnector::renderTranslateExecuteSql(
    connection,
    sql = sql,
    cdm_database_schema = dataSource$cdmDatabaseSchema,
    cohort_database_schema = config$cdmDatabase$schema,
    outcome_cohort_definition_table = config$cdmDatabase$outcomeCohortDefinitionTable,
    cdm_outcome_cohort_schema = dataSource$cdmOutcomeCohortSchema,
    cdm_outcome_cohort_table = dataSource$cdmOutcomeCohortTable,
    custom_outcome_cohort_list = customOutcomeCohortList
  )
}

createReferenceTables <- function(connection, config) {
  base::writeLines("Removing and inserting references")
  sql <- SqlRender::readSql(system.file("sql/create", "createReferenceTables.sql", package = "rewardb"))
  DatabaseConnector::renderTranslateExecuteSql(
    connection,
    sql = sql,
    cohort_database_schema = config$cdmDatabase$schema,
    conceptset_definition_table = config$cdmDatabase$conceptSetDefinitionTable,
    cohort_definition_table = config$cdmDatabase$cohortDefinitionTable,
    outcome_cohort_definition_table = config$cdmDatabase$outcomeCohortDefinitionTable
  )

  base::writeLines("Inserting ingredient/ATC cohorts")
  createTargetDefinitions(connection, config)
  
  sql <- SqlRender::readSql(system.file("sql/create", "outcomeCohortDefinitions.sql", package = "rewardb"))
  
  for (dataSource in config$dataSources) {
    
    DatabaseConnector::renderTranslateExecuteSql(
      connection,
      sql = sql,
      cdm_database_schema = dataSource$cdmDatabaseSchema,
      cohort_database_schema = config$cdmDatabase$schema,
      outcome_cohort_definition_table = config$cdmDatabase$outcomeCohortDefinitionTable
    )
    # TODO - move these to a reference table to allow adding them one by one
    #customOutcomeCohortList <- c(7542, 7551, 7552, 7553, 7576, 7543, 7545, 7546, 7507, 7547, 7548, 7549, 7550, 7822, 7823, 10357, 11073, 2538, 10593, 10605, 15078, 10607, 11643, 12047)

    #createAtlasReference(connection, config, dataSource, customOutcomeCohortList)
  }
}


execute <- function (configFilePath="config/global-cfg.yml") {
    # load config
    base::writeLines("Creating and populating reference tables...")
    config <- yaml::read_yaml(configFilePath)
    # createReferenceTables
    connection <- DatabaseConnector::connect(config$cdmDataSource)
    createReferenceTables(connection, config)

  # createCohorts

  #for (dataSource in config$dataSources) {

  # createCohorts

  # createOutcomeCohorts

  # createOutcomeSummary

  # run SCC

  # addCustomSccOutcome

  #}
}

# Combine results

# compile results