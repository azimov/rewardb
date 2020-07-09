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

  sql <- SqlRender::readSql(system.file("sql/create", "atlasCohortReferenceTables.sql", package = "rewardb"))
  DatabaseConnector::renderTranslateExecuteSql(
    connection,
    sql = sql,
    cohort_database_schema = config$cdmDatabase$schema,
    atlas_reference_table = config$cdmDatabase$atlasCohortReferenceTable,
    atlas_concept_reference = config$cdmDatabase$atlasConceptReferenceTable
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

    #createAtlasReference(connection, config, dataSource, customOutcomeCohortList)
  }
}

# Adds atlas cohort to db reference, from web api
# Inserts name/id in to custom cohort table
# Maps condition concepts of interest, any desecdants or if they're excluded from the cohort

addAtlasCohortConcepts <- function(connection, config, atlasId) {
  content <- rewardb::getWebObject(config$webApiUrl, "cohortdefinition", atlasId)
  cohortDef <- RJSONIO::fromJSON(content$expression)

  DatabaseConnector::renderTranslateExecuteSql(
    connection,
    sql = "INSERT INTO @cohort_database_schema.@atlas_reference_table (cohort_definition_id, cohort_name)
                                   values (@cohort_definition_id, '@cohort_name')",
    cohort_database_schema = config$cdmDatabase$schema,
    atlas_reference_table = config$cdmDatabase$atlasCohortReferenceTable,
    cohort_definition_id = cohortDef$id,
    cohort_name = cohortDef$name
  )

  sql <- "insert into @cohort_database_schema.@outcome_cohort_definition_table
  (
    cohort_definition_id,
    cohort_definition_name,
    short_name,
    CONCEPTSET_ID,
    outcome_type,
  )
  VALUES (
    @cohort_definition_id,
    'Custom outcome definition',
    'Custom outcome definition',
    99999999,
    2
  )"
  DatabaseConnector::renderTranslateExecuteSql(
    connection,
    sql=sql,
    cohort_definition_id = cohortDef$id,
    cohort_database_schema = config$cdmDatabase$schema,
    outcome_cohort_definition_table = config$cdmDatabase$outcomeCohortDefinitionTable
  )

  results <- data.frame()
  for (conceptSet in cohortDef$ConceptSets) {
    for (item in conceptSet$expression$items) {

      if (item$concept$DOMAIN_ID == "Condition" && !item$isExcluded) {

        results <- rbind(results, data.frame(
          ATLAS_ID = atlasId,
          CONCEPT_ID = item$concept$CONCEPT_ID,
          IS_EXCLUDED = item$isExcluded,
          include_descendants = item$includeDescendants
        )
        )
      }
    }
  }
  tableName <- paste0(config$cdmDatabase$schema, ".", config$cdmDatabase$atlasConceptReferenceTable)
  DatabaseConnector::dbAppendTable(connection, tableName, results)

}

execute <- function(configFilePath = "config/global-cfg.yml") {
  customOutcomeCohortList <- c(7542, 7551, 7552, 7553, 7576, 7543, 7545, 7546, 7507, 7547, 7548, 7549, 7550, 7822, 7823,
                               10357, 11073, 2538, 10593, 10605, 15078, 10607, 11643, 12047)
  # load config
  base::writeLines("Creating and populating reference tables...")
  config <- yaml::read_yaml(configFilePath)
  # createReferenceTables
  connection <- DatabaseConnector::connect(config$cdmDataSource)
  createReferenceTables(connection, config)

  # createCohorts
  # createOutcomeCohorts
  # createOutcomeSummary
  # run SCC
  # run SCCS
}

# Combine results

# compile results