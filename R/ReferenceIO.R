CONST_REFERENCE_TABLES <- c(
  "concept_set_definition",
  "cohort_definition",
  "outcome_cohort_definition",
  "atlas_outcome_reference",
  "atlas_concept_reference",
  "custom_exposure",
  "custom_exposure_concept",
  "analysis_setting"
)

CONST_EXCLUDE_REF_COLS <- list(
  "atlasOutcomeReference" = c("SQL_DEFINITION", "DEFINITION")
)

#' Export Reference tables
#' @description
#' Takes created reference tables (cohort definitions) from central rewardb and exports them to a zipped csv file
exportReferenceTables <- function(
  config,
  exportPath = tempdir(),
  exportZipFile = "rewardb-references.zip"
) {
  scipen <- getOption("scipen")
  options(scipen = 999)
  connection <- DatabaseConnector::connect(connectionDetails = config$connectionDetails)
  tryCatch(
  {
    # Collect all files and make a hash
    meta <- list()
    meta$hashList <- list()
    meta$tableNames <- CONST_REFERENCE_TABLES

    for (table in rewardb::CONST_REFERENCE_TABLES) {
      data <- DatabaseConnector::renderTranslateQuerySql(
        connection,
        "SELECT * FROM @schema.@table;",
        schema = config$rewardbResultsSchema,
        table = table
      )

      file <- file.path(exportPath, paste0(table, ".csv"))
      suppressWarnings({ write.csv(data, file, na = "", row.names = FALSE, fileEncoding = "ascii") })
      meta$hashList[[basename(file)]] <- tools::md5sum(file)[[1]]
    }

    metaDataFilename <- file.path(exportPath, rewardb::CONST_META_FILE_NAME)
    jsonlite::write_json(meta, metaDataFilename)

    exportFiles <- file.path(exportPath, paste0(rewardb::CONST_REFERENCE_TABLES, ".csv"))
    zip::zipr(exportZipFile, append(exportFiles, metaDataFilename), include_directories = FALSE)

    ParallelLogger::logInfo(paste("Created export zipfile", exportZipFile))
  },
    error = ParallelLogger::logError
  )
  DatabaseConnector::disconnect(connection)
  options(scipen = scipen)
}

#'
#' @description
#' Note that this always overwrites the existing reference tables stored in the database
importReferenceTables <- function(cdmConfig, zipFilePath, usePgCopy = FALSE) {
  unzipAndVerify(zipFilePath, cdmConfig$referencePath, TRUE)
  connection <- DatabaseConnector::connect(connectionDetails = cdmConfig$connectionDetails)

  tryCatch(
  {
    sql <- SqlRender::readSql(system.file("sql/create", "referenceTables.sql", package = "rewardb"))
    DatabaseConnector::renderTranslateExecuteSql(
      connection,
      sql,
      schema = cdmConfig$referenceSchema,
      concept_set_definition = cdmConfig$tables$conceptSetDefinition,
      cohort_definition = cdmConfig$tables$cohortDefinition,
      outcome_cohort_definition = cdmConfig$tables$outcomeCohortDefinition,
      atlas_outcome_reference = cdmConfig$tables$atlasOutcomeReference,
      atlas_concept_reference = cdmConfig$tables$atlasConceptReference,
      custom_exposure = cdmConfig$tables$customExposure,
      custom_exposure_concept = cdmConfig$tables$customExposureConcept,
      analysis_setting = cdmConfig$tables$analysisSetting
    )

    fileList <- file.path(cdmConfig$referencePath, paste0(rewardb::CONST_REFERENCE_TABLES, ".csv"))
    for (file in fileList) {
      camelName <- SqlRender::snakeCaseToCamelCase(strsplit(basename(file), ".csv")[[1]])
      tableName <- cdmConfig$tables[[camelName]]

      if (cdmConfig$connectionDetails$dbms == "postgresql" & usePgCopy) {
        ParallelLogger::logDebug(paste("Using pgcopy to upload", camelName, tableName, file))
        pgCopy(connectionDetails = cdmConfig$connectionDetails, csvFileName = file, schema = cdmConfig$referenceSchema, tableName = tableName)
      } else {
        ParallelLogger::logDebug(paste("Using insert table", camelName, tableName, file))
        data <- read.csv(file)

        # Remove columns we don't want to store on the CDM
        if (camelName %in% names(rewardb::CONST_EXCLUDE_REF_COLS)) {
          data <- data[, !(names(data) %in% rewardb::CONST_EXCLUDE_REF_COLS[[camelName]])]
          ParallelLogger::logDebug(names(data))
        }

        DatabaseConnector::insertTable(
          connection = connection,
          tableName = paste(cdmConfig$referenceSchema, tableName, sep = "."),
          data = data,
          progressBar = TRUE,
          dropTableIfExists = TRUE,
          useMppBulkLoad = cdmConfig$useMppBulkLoad,
          oracleTempSchema = cdmConfig$oracleTempSchema
        )
      }
    }

  },
    error = function(err) {
      ParallelLogger::logError(err)
      return(NULL)
    }
  )
  DatabaseConnector::disconnect(connection)
}

registerCdm <- function(connection, globalConfig, cdmConfig) {
  # Check if CDM ID is in table
  # Check if source key is a match
  # Add it to the list
  sql <- "SELECT count(*) as cnt FROM @schema.data_source WHERE source_id = @source_id"
  res <- DatabaseConnector::renderTranslateQuerySql(connection, sql, schema = globalConfig$rewardbResultsSchema, source_id = cdmConfig$sourceId)

  if (res$CNT[[1]] == 0) {
    sql <- "insert into @schema.data_source (source_id, source_key, source_name)
              values (@source_id, '@source_key', '@source_name');"
    DatabaseConnector::renderTranslateExecuteSql(connection,
                                                 sql,
                                                 schema = globalConfig$rewardbResultsSchema,
                                                 source_id = cdmConfig$sourceId,
                                                 source_name = cdmConfig$name,
                                                 source_key = cdmConfig$database)
  }
}

#' Exports atlas cohort references to a zip file for import in to an already existing db
#' #TODO
#' @param connection DatabaseConnector::connection to postgres
#' @param config rewardb global config
#' @param atlasIds ids to atlas cohort to pull down
exportAtlasCohortRef <- function(
  config,
  atlasIds,
  exportZipFile,
  atlasUrl = NULL,
  exportPath = tempdir()
) {
  scipen <- getOption("scipen")
  options(scipen = 999)
  connection <- DatabaseConnector::connect(connectionDetails = config$connectionDetails)
  tryCatch(
  {
    # Get the cohort definition ids for these atlas cohorts
    sql <- "
      SELECT cohort_definition_id FROM @schema.atlas_outcome_reference
      WHERE atlas_id IN (@atlas_ids)
      {@atlas_url != ''} ? {AND atlas_url = '@atlas_url'}
    "
    cohortDefinitionIds <- renderTranslateQuerySql(connection,
                                                   sql,
                                                   schema = config$rewardbResultsSchema,
                                                   atlas_ids = atlasIds,
                                                   atlas_url = atlasUrl)$COHORT_DEFINITION_ID


    # Collect all files and make a hash
    meta <- list(atlasIds = atlasIds, cohortDefinitionIds = cohortDefinitionIds)
    meta$hashList <- list()

    outputTables <- c(
      "outcome_cohort_definition" = "SELECT * FROM @schema.outcome_cohort_definition WHERE cohort_definition_id IN (@cohort_definition_ids)",
      "atlas_outcome_reference" = "SELECT * FROM @schema.atlas_outcome_reference WHERE cohort_definition_id IN (@cohort_definition_ids)",
      "atlas_concept_reference" = "SELECT * FROM @schema.atlas_concept_reference WHERE cohort_definition_id IN (@cohort_definition_ids)"
    )

    meta$tableNames <- names(outputTables)

    for (table in names(outputTables)) {
      data <- DatabaseConnector::renderTranslateQuerySql(
        connection,
        outputTables[[table]],
        schema = config$rewardbResultsSchema,
        cohort_definition_ids = cohortDefinitionIds
      )

      file <- file.path(exportPath, paste0(table, ".csv"))
      suppressWarnings({ write.csv(data, file, na = "", row.names = FALSE, fileEncoding = "ascii") })
      meta$hashList[[basename(file)]] <- tools::md5sum(file)[[1]]
    }

    metaDataFilename <- file.path(exportPath, rewardb::CONST_META_FILE_NAME)
    jsonlite::write_json(meta, metaDataFilename)

    exportFiles <- file.path(exportPath, paste0(names(outputTables), ".csv"))
    zip::zipr(exportZipFile, append(exportFiles, metaDataFilename), include_directories = FALSE)

    ParallelLogger::logInfo(paste("Created export zipfile", exportZipFile))
  },
    error = ParallelLogger::logError
  )
  DatabaseConnector::disconnect(connection)
  options(scipen = scipen)
}

importAtlasCohortReferencesZip <- function(cdmConfig, zipFilePath, exportPath) {
  unzipAndVerify(zipFilePath, exportPath, TRUE)
  connection <- DatabaseConnector::connect(connectionDetails = cdmConfig$connectionDetails)

  tryCatch(
  {

    inputTables <- c(
      "outcome_cohort_definition",
      "atlas_outcome_reference",
      "atlas_concept_reference"
    )
    fileList <- file.path(exportPath, paste0(inputTables, ".csv"))
    for (file in fileList) {
      camelName <- SqlRender::snakeCaseToCamelCase(strsplit(basename(file), ".csv")[[1]])
      tableName <- cdmConfig$tables[[camelName]]

      ParallelLogger::logDebug(paste("Using insert table", camelName, tableName, file))
      data <- read.csv(file)

      # Remove columns we don't want to store on the CDM
      if (camelName %in% names(rewardb::CONST_EXCLUDE_REF_COLS)) {
        data <- data[, !(names(data) %in% rewardb::CONST_EXCLUDE_REF_COLS[[camelName]])]
      }

      insertTable(
        connection = connection,
        tableName = paste(cdmConfig$referenceSchema, tableName, sep = "."),
        data = data,
        progressBar = TRUE,
        dropTableIfExists = TRUE,
        useMppBulkLoad = cdmConfig$useMppBulkLoad,
        oracleTempSchema = cdmConfig$oracleTempSchema
      )
    }

  },
    error = function(err) {
      ParallelLogger::logError(err)
      return(NULL)
    }
  )
  DatabaseConnector::disconnect(connection)
}