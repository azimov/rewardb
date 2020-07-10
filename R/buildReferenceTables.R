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

addAtlasCohort <- function(connection, config, atlasId) {
  count <- DatabaseConnector::renderTranslateQuerySql(
    connection,
    "SELECT COUNT(*) FROM @cohort_database_schema.@atlas_reference_table WHERE cohort_definition_id = @cohort_definition_id",
    cohort_database_schema = config$cdmDatabase$schema,
    atlas_reference_table = config$cdmDatabase$atlasCohortReferenceTable,
    cohort_definition_id = content$id
  )

  if (count == 0) {
    content <- rewardb::getWebObject(config$webApiUrl, "cohortdefinition", atlasId)
    cohortDef <- RJSONIO::fromJSON(content$expression)
    DatabaseConnector::renderTranslateExecuteSql(
      connection,
      sql = "INSERT INTO @cohort_database_schema.@atlas_reference_table (cohort_definition_id, cohort_name)
                                     values (@cohort_definition_id, '@cohort_name')",
      cohort_database_schema = config$cdmDatabase$schema,
      atlas_reference_table = config$cdmDatabase$atlasCohortReferenceTable,
      cohort_definition_id = content$id,
      cohort_name = gsub("'","''", content$name)
    )

    sql <- SqlRender::readSql(system.file("sql/create", "customAtlasCohorts.sql", package = "rewardb"))
    DatabaseConnector::renderTranslateExecuteSql(
      connection,
      sql=sql,
      cohort_definition_id = content$id,
      custom_outcome_name = gsub("'","''", content$name),
      subject_id = "",
      cohort_start_date = "",
      cohort_end_data = "",
      cohort_database_schema = config$cdmDatabase$schema,
      outcome_cohort_definition_table = config$cdmDatabase$outcomeCohortDefinitionTable
    )

    results <- data.frame()
    for (conceptSet in cohortDef$ConceptSets) {
      for (item in conceptSet$expression$items) {
        if (item$concept$DOMAIN_ID == "Condition") {
          results <- rbind(results, data.frame(
            COHORT_DEFINITION_ID = content$id,
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
  } else {
    print(paste("COHORT", atlasId, "Already in database, use removeAtlasCohort to clear entry references"))
  }
}

removeAtlasCohort <- function (connection, config, atlasId) {
  DatabaseConnector::renderTranslateExecuteSql(
    connection,
    sql = "DELETE FROM @cohort_database_schema.@outcome_cohort_definition_table WHERE cohort_definition_id = @cohort_definition_id;
    DELETE FROM @cohort_database_schema.@atlas_concept_reference WHERE cohort_definition_id = @cohort_definition_id;
    DELETE FROM @cohort_database_schema.@atlas_reference_table WHERE cohort_definition_id = @cohort_definition_id;",
    cohort_database_schema = config$cdmDatabase$schema,
    outcome_cohort_definition_table = config$cdmDatabase$outcomeCohortDefinitionTable,
    cohort_definition_id = atlasId,
    atlas_reference_table = config$cdmDatabase$atlasCohortReferenceTable,
    atlas_concept_reference = config$cdmDatabase$atlasConceptReferenceTable
  )
}

buildReferenceTables <- function(configFilePath = "config/global-cfg.yml") {
  # load config
  base::writeLines("Creating and populating reference tables...")
  config <- yaml::read_yaml(configFilePath)
  # createReferenceTables
  connection <- DatabaseConnector::connect(config$cdmDataSource)
  createReferenceTables(connection, config)

  for (aid in conifg$maintinedAtlasCohortList) {
    removeAtlasCohort(connection, config, aid)
    addAtlasCohort(connection, config, aid)
  }
  # createCohorts
  # createOutcomeCohorts
  # createOutcomeSummary
  # run SCC
  # run SCCS
}

# Combine results

# compile results