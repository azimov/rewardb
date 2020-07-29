# Hit a web api REST endpoint
# TODO: replace with ROhdsiWebApi calls
#'@export
getWebObject <- function(webApiUrl, resource, id) {
  definitionUrl <- URLencode(paste0(webApiUrl, "/", resource, "/", id))
  resp <- httr::GET(definitionUrl)
  content <- httr::content(resp, as = "text", encoding = "UTF-8")
  responseData <- RJSONIO::fromJSON(content)
  return(responseData)
}

# Adds atlas cohort to db reference, from web api
# Inserts name/id in to custom cohort table
# Maps condition concepts of interest, any desecdants or if they're excluded from the cohort
insertAtlasCohortRef <- function(connection, config, atlasId) {

  ParallelLogger::logInfo(paste("Checking if cohort already exists", atlasId))
  count <- DatabaseConnector::renderTranslateQuerySql(
    connection,
    "SELECT COUNT(*) FROM @cohort_database_schema.@atlas_reference_table WHERE cohort_definition_id = @cohort_definition_id",
    cohort_database_schema = config$cdmDatabase$schema,
    atlas_reference_table = config$cdmDatabase$atlasCohortReferenceTable,
    cohort_definition_id = atlasId
  )

  if (count == 0) {
    ParallelLogger::logInfo(paste("pulling", atlasId))
    content <- rewardb::getWebObject(config$webApiUrl, "cohortdefinition", atlasId)
    cohortDef <- RJSONIO::fromJSON(content$expression)

    ParallelLogger::logInfo(paste("inserting", atlasId))
    DatabaseConnector::renderTranslateExecuteSql(
      connection,
      sql = "INSERT INTO @cohort_database_schema.@atlas_reference_table (cohort_definition_id, cohort_name)
                                     values (@cohort_definition_id, '@cohort_name')",
      cohort_database_schema = config$cdmDatabase$schema,
      atlas_reference_table = config$cdmDatabase$atlasCohortReferenceTable,
      cohort_definition_id = content$id,
      cohort_name = gsub("'","''", content$name)
    )

    ParallelLogger::logInfo(paste("Getting concept sets", atlasId))
    sql <- SqlRender::readSql(system.file("sql/create", "customAtlasCohortsConcepts.sql", package = "rewardb"))
    DatabaseConnector::renderTranslateExecuteSql(
      connection,
      sql=sql,
      cohort_definition_id = content$id,
      custom_outcome_name = gsub("'","''", content$name),
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

removeAtlasCohort <- function (connection, config, atlasIds, dataSources) {
  DatabaseConnector::renderTranslateExecuteSql(
    connection,
    sql = "DELETE FROM @cohort_database_schema.@outcome_cohort_definition_table WHERE cohort_definition_id IN (@cohort_definition_id);
    DELETE FROM @cohort_database_schema.@atlas_concept_reference WHERE cohort_definition_id IN (@cohort_definition_id);
    DELETE FROM @cohort_database_schema.@atlas_reference_table WHERE cohort_definition_id IN (@cohort_definition_id);",
    cohort_database_schema = config$cdmDatabase$schema,
    outcome_cohort_definition_table = config$cdmDatabase$outcomeCohortDefinitionTable,
    cohort_definition_id = atlasIds,
    atlas_reference_table = config$cdmDatabase$atlasCohortReferenceTable,
    atlas_concept_reference = config$cdmDatabase$atlasConceptReferenceTable
  )

  for (ds in dataSources) {
    dataSource <- config$dataSources[[ds]]
    DatabaseConnector::renderTranslateExecuteSql(
      connection,
      sql = "DELETE FROM @cohort_database_schema.@outcome_cohort_table WHERE cohort_definition_id IN (@cohort_definition_id)",
      cohort_definition_id = atlasIds,
      cohort_database_schema = config$cdmDatabase$schema,
      outcome_cohort_table = dataSource$outcomeCohortTable
    )
  }
}