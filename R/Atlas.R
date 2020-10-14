#' Adds atlas cohort to db reference, from web api
#' Inserts name/id in to custom cohort table
#' Maps condition concepts of interest, any desecdants or if they're excluded from the cohort
#' @param connection DatabaseConnector::connection to cdm
#' @param config rewardb global config
#' @param atlasId id to atlas cohort to pull down
insertAtlasCohortRef <- function(
  connection,
  config,
  atlasId,
  webApiUrl = NULL,
  cohortDefinition = NULL,
  sqlDefinition = NULL
) {

  if (is.null(webApiUrl)) {
    webApiUrl <- config$webApiUrl
  }

  ParallelLogger::logInfo(paste("Checking if cohort already exists", atlasId))
  count <- DatabaseConnector::renderTranslateQuerySql(
    connection,
    "SELECT COUNT(*) FROM @schema.atlas_outcome_reference
        WHERE atlas_id = @atlas_id
        AND atlas_url = '@atlas_url'
        ",
    schema = config$rewardbResultsSchema,
    atlas_id = atlasId,
    atlas_url = webApiUrl
  )

  if (count == 0) {
    ParallelLogger::logInfo(paste("pulling", atlasId))
    # Null is mainly used for test purposes only
    if(is.null(cohortDefinition)) {
      cohortDefinition <- ROhdsiWebApi::getCohortDefinition(atlasId, webApiUrl)
    }

    if(is.null(sqlDefinition)) {
       sqlDefinition <- ROhdsiWebApi::getCohortSql(cohortDefinition, webApiUrl, generateStats = FALSE)
    }

    ParallelLogger::logInfo(paste("inserting", atlasId))
    # Create reference and Get last insert as referent ID from sequence
    newEntry <- DatabaseConnector::renderTranslateQuerySql(
      connection,
      sql = "INSERT INTO @schema.outcome_cohort_definition
                            (cohort_definition_name, short_name, CONCEPTSET_ID, outcome_type)
                                     values ('@name', '@name', 99999999, 2) RETURNING cohort_definition_id",
      schema = config$rewardbResultsSchema,
      name = gsub("'","''", cohortDefinition$name)
    )

    cohortDefinitionId <- newEntry$COHORT_DEFINITION_ID[[1]]

    DatabaseConnector::renderTranslateExecuteSql(
      connection,
      sql = "INSERT INTO @schema.atlas_outcome_reference
                            (cohort_definition_id, ATLAS_ID, atlas_url, definition, sql_definition)
                                     values (@cohort_definition_id, @atlas_id, '@atlas_url', '@definition', '@sql_definition')",
      schema = config$rewardbResultsSchema,
      cohort_definition_id = cohortDefinitionId,
      atlas_id = atlasId,
      atlas_url = gsub("'","''", webApiUrl),
      definition = gsub("'","''", RJSONIO::toJSON(cohortDefinition)),
      sql_definition = gsub("'","''", sqlDefinition),
    )

    ParallelLogger::logInfo(paste("inserting concept reference", atlasId))
    results <- data.frame()
    for (conceptSet in cohortDefinition$expression$ConceptSets) {
      for (item in conceptSet$expression$items) {
        if (item$concept$DOMAIN_ID == "Condition") {
          results <- rbind(results, data.frame(
            COHORT_DEFINITION_ID = cohortDefinitionId,
            CONCEPT_ID = item$concept$CONCEPT_ID,
            IS_EXCLUDED = as.integer(item$isExcluded),
            INCLUDE_MAPPED = as.integer(item$includeMapped),
            include_descendants = as.integer(item$includeDescendants)
          )
          )
        }
      }
    }
    tableName <- paste0(config$rewardbResultsSchema, ".atlas_concept_reference")
    DatabaseConnector::dbAppendTable(connection, tableName, results)
  } else {
    print(paste("COHORT", atlasId, "Already in database, use removeAtlasCohort to clear entry references"))
  }
}

#' Removes atlas entries from the cdm store
#' @param connection DatabaseConnector::connection to cdm
#' @param config rewardb global config
#' @param atlasIds ids to remove from db
#' @param dataSources specified data sources to remove entry from
removeAtlasCohort <- function (connection, config, atlasId, webApiUrl = NULL) {
  if (is.null(webApiUrl)) {
    webApiUrl <- config$webApiUrl
  }

  DatabaseConnector::renderTranslateExecuteSql(
    connection,
    "DELETE FROM @schema.outcome_cohort_definition WHERE cohort_definition_id
            IN ( SELECT cohort_definition_id FROM @schema.atlas_outcome_reference
            WHERE atlas_id = @atlas_id AND atlas_url = '@atlas_url');",
    schema = config$rewardbResultsSchema,
    atlas_id = atlasId,
    atlas_url = webApiUrl
  )
}

#' Adds atlas concept set to db reference, from web api
#' Inserts name/id in to custom cohort table
#' Maps condition concepts of interest, any desecdants or if they're excluded from the cohort
#' @param connection DatabaseConnector::connection to cdm
#' @param config rewardb global config
#' @param conceptSetId id to atlas cohort to pull down
#' @param conceptSetDefinition webApi response object
insertCustomExposureRef <- function(
  connection,
  config,
  conceptSetId,
  cohortName,
  conceptSetDefinition = NULL,
  .warnNonIngredients = TRUE,
  webApiUrl = NULL
) {

  if (is.null(webApiUrl)) {
    webApiUrl <- config$webApiUrl
  }

  ParallelLogger::logInfo(paste("Checking if cohort already exists", conceptSetId))

  if (is.null(conceptSetDefinition)) {
    conceptSetDefinition <- ROhdsiWebApi::getConceptSetDefinition(conceptSetId, webApiUrl)
  }

  count <- DatabaseConnector::renderTranslateQuerySql(
    connection,
    "SELECT COUNT(*) FROM @schema.custom_exposure WHERE CONCEPT_SET_ID = @conceptSetId AND atlas_url = '@atlas_url'",
    schema = config$rewardbResultsSchema,
    atlas_url = webApiUrl,
    conceptSetId = conceptSetId
  )

  if (count == 0) {
    ParallelLogger::logInfo(paste("inserting", conceptSetId))

    newEntry <- DatabaseConnector::renderTranslateQuerySql(
      connection,
      sql = "INSERT INTO @schema.cohort_definition
                            (cohort_definition_name, short_name, atc_flg)
                                     values ('@name', '@name', 2) RETURNING cohort_definition_id",
      schema = config$rewardbResultsSchema,
      name = gsub("'","''", cohortName),
    )

    cohortDefinitionId <- newEntry$COHORT_DEFINITION_ID[[1]]

    DatabaseConnector::renderTranslateExecuteSql(
      connection,
      "INSERT INTO @schema.custom_exposure
                (COHORT_DEFINITION_ID, CONCEPT_SET_ID, ATLAS_URL) VALUES (@cohort_definition_id, @concept_set_id, '@atlas_url')",
      schema = config$rewardbResultsSchema,
      cohort_definition_id = cohortDefinitionId,
      concept_set_id = conceptSetId,
      atlas_url = webApiUrl
    )

    results <- data.frame()
    for (item in conceptSetDefinition$expression$items) {
      if (item$concept$CONCEPT_CLASS_ID != "Ingredient" & .warnNonIngredients) {
        ParallelLogger::logWarn(paste("Inserting a non-ingredient concept", item$concept$conceptId, "for cohort", cohortName))
      }

      results <- rbind(
        results,
        data.frame(
          COHORT_DEFINITION_ID = cohortDefinitionId,
          CONCEPT_ID = item$concept$CONCEPT_ID,
          IS_EXCLUDED = as.integer(item$isExcluded),
          include_descendants = as.integer(item$includeDescendants),
          include_mapped = as.integer(item$includeMapped)
        )
      )
    }

    tableName <- paste0(config$rewardbResultsSchema, ".custom_exposure_concept")
    DatabaseConnector::dbAppendTable(connection, tableName, results)
  } else {
    print(paste("Concept set", conceptSetId, "Already in database, use removeAtlasCohort to clear entry references"))
  }
}

#' Removes atlas entries from the cdm store
#' @param connection DatabaseConnector::connection to cdm
#' @param config rewardb global config
#' @param conceptSetId ids to remove from db
#' @param dataSources specified data sources to remove entry from
removeCustomExposureCohort <- function (connection, config, conceptSetId, webApiUrl = NULL) {
  if (is.null(webApiUrl)) {
    webApiUrl <- config$webApiUrl
  }

  DatabaseConnector::renderTranslateExecuteSql(
    connection,
    "DELETE FROM @schema.cohort_definition WHERE cohort_definition_id
            IN ( SELECT cohort_definition_id FROM @schema.custom_exposure
              WHERE concept_set_id = @concept_set_id AND atlas_url = '@atlas_url');",
    schema = config$rewardbResultsSchema,
    concept_set_id = conceptSetId,
    atlas_url = webApiUrl
  )
}