#' @title
#' add Phenotype Library Cohorts
#' @description
#' Add the OHDSI phenotype library references to the db.
#' This method heavily depends on the format of the Phenotype Library staying solid which is unlikely at this stage.
#' By default, uses remotes::install_github but can be set to a custom, local git repository
#' @param connection DatabaseConnector::connection to postgres
#' @param config Global rewardb config object
#' @param libraryRepo Path to the repository. If local = TRUE path to custom git should be set here. Otherwise must specify a github repo
#' @param ref git ref to use. Defaults to master. could be a specific git tag or branch
#' @param local local copy or github
#' @param packageName name of package to install
#' @param removeExisting remove existing pacakge of the same name
#' @param generateSql use web api to generate sql from source
addPhenotypeLibrary <- function(connection,
                                config,
                                libraryRepo = "OHDSI/PhenotypeLibrary",
                                ref = "master",
                                local = FALSE,
                                packageName = "PhenotypeLibrary",
                                removeExisting = FALSE,
                                generateSql = TRUE) {
  # Go through folders and add each cohort with names and descriptions
  if (local) {
    remotes::install_git(libraryRepo, ref = ref, force = TRUE)
  } else {
    remotes::install_github(libraryRepo, ref = ref, force = TRUE)
  }

  definitions <- Sys.glob(file.path(system.file("", package = packageName), "*", "*.json"))

  failures <- list()

  for (jsonDefinition in definitions) {
    # Folder
    directory <- stringr::str_split(jsonDefinition, basename(jsonDefinition))[[1]][[1]]

    tryCatch({
      description <- readr::read_csv(file.path(directory, "cohortDescription.csv"))
      cohortDefinition <- list()

      cohortId <- stringr::str_split(basename(jsonDefinition), ".json")[[1]][[1]]

      cohortDefinition$name <- description[description$cohortId == cohortId,]$cohortName[[1]]
      cohortDefinition$description <- description[description$cohortId == cohortId,]$logicDescription[[1]]
      cohortDefinition$id <- cohortId
      cohortDefinition$expression <- RJSONIO::fromJSON(jsonDefinition)

      if (generateSql) {
        sqlDefinition <- ROhdsiWebApi::getCohortSql(cohortDefinition, config$webApiUrl, generateStats = FALSE)
      } else {
        sqlPath <- paste0(stringr::str_split(jsonDefinition, ".json")[[1]][[1]], ".sql")
        sqlDefinition <- readr::read_file(sqlPath)
      }

      urlRef <- paste(libraryRepo, ref, sep = "@")
      if (removeExisting) {
        removeAtlasCohort(connection, config, cohortId, urlRef)
      }

      insertAtlasCohortRef(connection, config, cohortId,
                           webApiUrl = urlRef,
                           cohortDefinition = cohortDefinition, sqlDefinition = sqlDefinition)
    },
      error = function(error) {
        ParallelLogger::logError("Failed to add ", jsonDefinition)
        ParallelLogger::logError(error)
        failures <- append(failures, jsonDefinition)
      })
  }

  if (length(failures)) {
    ParallelLogger::logError(paste("Failed to add", failures))
  }
}

#' @title
#' Insert atlas cohort ref to postgres db
#' @description
#' Adds atlas cohort to db reference, from web api
#' Inserts name/id in to custom cohort table
#' Maps condition and drug concepts of interest, any desecdants or if they're excluded from the cohort
#' Concepts not from drug domain are ignored for exposures, concepts not from condition domain are ignored for outcomes
#' @param connection DatabaseConnector::connection to postgres
#' @param config rewardb global config
#' @param atlasId id to atlas cohort to pull down
#' @param exposure If exposure, cohort is treated as an exposure, drug domains are captured
#'
#' @export
insertAtlasCohortRef <- function(
  connection,
  config,
  atlasId,
  webApiUrl = NULL,
  cohortDefinition = NULL,
  sqlDefinition = NULL,
  exposure = FALSE
) {
  if (is.null(webApiUrl)) {
    webApiUrl <- config$webApiUrl
  }

  if (exposure) {
    cohortTable <- "cohort_definition"
    referenceTable <- "atlas_exposure_reference"
    conceptTable <- "atlas_exposure_concept"
    insertSql <- "INSERT INTO @schema.@cohort_table
                            (cohort_definition_name, short_name, drug_conceptset_id, atc_flg, target_cohort, subgroup_cohort)
                                     values ('@name', '@name', 99999999, -1, 0, 0) RETURNING cohort_definition_id"
  } else {
    cohortTable <- "outcome_cohort_definition"
    referenceTable <- "atlas_outcome_reference"
    conceptTable <- "atlas_outcome_concept"
    insertSql <- "INSERT INTO @schema.@cohort_table
                            (cohort_definition_name, short_name, conceptset_id, outcome_type)
                                     values ('@name', '@name', 99999999, 2) RETURNING cohort_definition_id"
  }


  ParallelLogger::logInfo(paste("Checking if cohort already exists", atlasId))
  count <- DatabaseConnector::renderTranslateQuerySql(
    connection,
    "SELECT COUNT(*) FROM @schema.@reference_table
        WHERE atlas_id = @atlas_id
        AND atlas_url = '@atlas_url'
        ",
    schema = config$rewardbResultsSchema,
    atlas_id = atlasId,
    atlas_url = webApiUrl,
    reference_table = referenceTable
  )

  if (count == 0) {
    ParallelLogger::logInfo(paste("pulling", atlasId))
    # Null is mainly used for test purposes only
    if (is.null(cohortDefinition)) {
      cohortDefinition <- ROhdsiWebApi::getCohortDefinition(atlasId, webApiUrl)
    }

    if (is.null(sqlDefinition)) {
      sqlDefinition <- ROhdsiWebApi::getCohortSql(cohortDefinition, webApiUrl, generateStats = FALSE)
    }

    ParallelLogger::logInfo(paste("inserting", atlasId))
    # Create reference and Get last insert as referent ID from sequence
    newEntry <- DatabaseConnector::renderTranslateQuerySql(
      connection,
      sql = insertSql,
      schema = config$rewardbResultsSchema,
      name = gsub("'", "''", cohortDefinition$name),
      cohort_table = cohortTable
    )

    cohortDefinitionId <- newEntry$COHORT_DEFINITION_ID[[1]]

    encodedFormDescription <- base64enc::base64encode(charToRaw(RJSONIO::toJSON(cohortDefinition)))
    encodedFormSql <- base64enc::base64encode(charToRaw(sqlDefinition))

    DatabaseConnector::renderTranslateExecuteSql(
      connection,
      sql = "INSERT INTO @schema.@reference_table
                            (cohort_definition_id, ATLAS_ID, atlas_url, definition, sql_definition)
                                     values (@cohort_definition_id, @atlas_id, '@atlas_url', '@definition', '@sql_definition')",
      schema = config$rewardbResultsSchema,
      cohort_definition_id = cohortDefinitionId,
      atlas_id = atlasId,
      atlas_url = gsub("'", "''", webApiUrl),
      definition = encodedFormDescription,
      sql_definition = encodedFormSql,
      reference_table = referenceTable
    )

    ParallelLogger::logInfo(paste("inserting concept reference", atlasId))
    results <- data.frame()
    for (conceptSet in cohortDefinition$expression$ConceptSets) {
      for (item in conceptSet$expression$items) {
        if ((!exposure & item$concept$DOMAIN_ID == "Condition") | (exposure & item$concept$DOMAIN_ID == "Drug")) {
          isExcluded <- if (is.null(item$isExcluded)) 0 else as.integer(item$isExcluded)
          includeDescendants <- if (is.null(item$includeDescendants)) 0 else as.integer(item$includeDescendants)
          includeMapped <- if (is.null(item$includeMapped)) 0 else as.integer(item$includeMapped)
          results <- rbind(results, data.frame(
              COHORT_DEFINITION_ID = cohortDefinitionId,
              CONCEPT_ID = item$concept$CONCEPT_ID,
              IS_EXCLUDED = isExcluded,
              INCLUDE_MAPPED = includeMapped,
              include_descendants = includeDescendants
            )
          )
        }
      }
    }
    if (length(results)) {
      tableName <- paste(config$rewardbResultsSchema, conceptTable, sep = ".")
      DatabaseConnector::dbAppendTable(connection, tableName, results)
    } else {
      warning("No Condition (outcome cohort) or Drug (exposure) domain references, automated negative control selection will fail for this cohort")
    }
    
  } else {
    warning(paste("COHORT", atlasId, "Already in database, use removeAtlasCohort to clear entry references"))
  }
}

#' @title
#' Insert atlas cohort ref to postgres db
#' @description
#' Removes atlas entries from the cdm store
#' @param connection DatabaseConnector::connection to cdm
#' @param config rewardb global config
#' @param atlasIds ids to remove from db
#' @param dataSources specified data sources to remove entry from
#' @param exposure is an exposure cohort or not
#'
#' @export
removeAtlasCohort <- function(connection, config, atlasId, webApiUrl = NULL, exposure = FALSE) {
  if (is.null(webApiUrl)) {
    webApiUrl <- config$webApiUrl
  }

  if (exposure) {
    cohortTable <- "cohort_definition"
    referenceTable <- "atlas_exposure_reference"
  } else {
    cohortTable <- "outcome_cohort_definition"
    referenceTable <- "atlas_outcome_reference"
  }

  DatabaseConnector::renderTranslateExecuteSql(
    connection,
    "DELETE FROM @schema.@cohort_table WHERE cohort_definition_id
            IN ( SELECT cohort_definition_id FROM @schema.@reference_table
            WHERE atlas_id = @atlas_id AND atlas_url = '@atlas_url');",
    schema = config$rewardbResultsSchema,
    atlas_id = atlasId,
    atlas_url = webApiUrl,
    cohort_table = cohortTable,
    reference_table = referenceTable
  )
}

#' @title
#' Insert atlas concept set to postgres db for a custom exposure cohort
#' @description
#' Adds atlas concept set to db reference, from web api
#' Inserts name/id in to custom cohort table
#' Maps condition concepts of interest, any desecdants or if they're excluded from the cohort
#' deprecated
#' Use expsosure = TRUE with atlas cohort definitions and add them will be removed in future version
#' @param connection DatabaseConnector::connection to cdm
#' @param config rewardb global config
#' @param conceptSetId id to atlas cohort to pull down
#' @param conceptSetDefinition webApi response object
#' @param cohortName Name to give cohort
#' @param .warnNonIngredients Warn for non ingredients
#' @param webApiUrl URL of webAPI service (if different from specified in config)
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
      name = gsub("'", "''", cohortName),
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

      isExcluded <- if (is.null(item$isExcluded)) 0 else as.integer(item$isExcluded)
      includeDescendants <- if (is.null(item$includeDescendants)) 0 else as.integer(item$includeDescendants)
      includeMapped <- if (is.null(item$includeMapped)) 0 else as.integer(item$includeMapped)

      results <- rbind(
        results,
        data.frame(
          COHORT_DEFINITION_ID = cohortDefinitionId,
          CONCEPT_ID = item$concept$CONCEPT_ID,
          IS_EXCLUDED = isExcluded,
          include_descendants = includeDescendants,
          include_mapped = includeMapped
        )
      )
    }

    tableName <- paste0(config$rewardbResultsSchema, ".custom_exposure_concept")
    DatabaseConnector::dbAppendTable(connection, tableName, results)
  } else {
    ParallelLogger::logDebug(paste("Concept set", conceptSetId, "Already in database, use removeAtlasCohort to clear entry references"))
  }
}

#' @title
#' Remove exposure conceptset cohort
#' @description
#' Removes atlas entries from the cdm store
#' deprecated
#' Use expsosure = TRUE with atlas cohort definitions and add them will be removed in future version
#' @param connection DatabaseConnector::connection to cdm
#' @param config rewardb global config
#' @param conceptSetId ids to remove from db
#' @param webApiUrl URL of WebApi resource (or other source if not imported via web api)
removeCustomExposureCohort <- function(connection, config, conceptSetId, webApiUrl = NULL) {
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

#' @title
#' Generate scc results for a one off atlas cohort (added after a full results run)
#' @description
#' Removes atlas entries from the cdm store
#' @param cdmConfig cdmConfig object loaded with loadCdmConfig
#' @param referenceZipFile path to ouputed zipFile of references if they need to be updated
#' @param configId String configuration id
#' @param exposure boolean - is this for exposure or outcome cohorts
#' @export
sccAdHocCohorts <- function(cdmConfigPath, configId, atlasIds, sourceUrl, exposure = FALSE, referenceZipFile = NULL) {
  cdmConfig <- loadCdmConfig(cdmConfigPath)

  if (!is.null(referenceZipFile)) {
    importReferenceTables(cdmConfig, referenceZipFile)
  }

  if (exposure) {
    referenceFile <- "atlas_exposure_reference.csv"
    targetCohortTable <- cdmConfig$tables$cohort
  } else {
    referenceFile <- "atlas_outcome_reference.csv"
    targetCohortTable <- cdmConfig$tables$outcomeCohort
  }

  connection <- DatabaseConnector::connect(cdmConfig$connection)
  # Create the cohort
  atlasCohorts <- read.csv(file.path(cdmConfig$referencePath, referenceFile))
  atlasCohorts <- atlasCohorts[atlasCohorts$ATLAS_ID %in% atlasIds & atlasCohorts$ATLAS_URL == sourceUrl,]

  if (length(atlasCohorts)) {
    # Generate each cohort
    apply(atlasCohorts, 1, function(cohortReference) {
      ParallelLogger::logInfo("computing custom cohort: ", cohortReference["COHORT_DEFINITION_ID"])
      DatabaseConnector::renderTranslateExecuteSql(
        connection,
        sql = "DELETE FROM @target_database_schema.@target_cohort_table WHERE cohort_definition_id = @cohort_definition_id",
        target_database_schema = cdmConfig$resultSchema,
        target_cohort_table = targetCohortTable,
        cohort_definition_id = cohortReference["COHORT_DEFINITION_ID"]
      )

      DatabaseConnector::renderTranslateExecuteSql(
        connection,
        sql = rawToChar(base64enc::base64decode(cohortReference["SQL_DEFINITION"])),
        cdm_database_schema = cdmConfig$cdmSchema,
        vocabulary_database_schema = cdmConfig$vocabularySchema,
        target_database_schema = cdmConfig$resultSchema,
        target_cohort_table = targetCohortTable,
        target_cohort_id = cohortReference["COHORT_DEFINITION_ID"]
      )
    })
  } else {
    stop("No cohorts specified to compute found in reference file.")
  }

  params <- list(
    cdmConfigPath = cdmConfigPath,
    configId = configId
  )

  if (exposure) {
    params$targetCohortIds <- atlasCohorts$COHORT_DEFINITION_ID
  } else {
    params$outcomeCohortIds <- atlasCohorts$COHORT_DEFINITION_ID
  }

  do.call(runAdHocScc, params)
}