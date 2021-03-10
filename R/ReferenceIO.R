CONST_REFERENCE_TABLES <- c(
  "concept_set_definition",
  "cohort_definition",
  "outcome_cohort_definition",
  "atlas_outcome_reference",
  "atlas_outcome_concept",
  "atlas_exposure_reference",
  "atlas_exposure_concept",
  "custom_exposure",
  "custom_exposure_concept",
  "analysis_setting"
)

CONST_EXCLUDE_REF_COLS <- list(
  "atlasOutcomeReference" = c("SQL_DEFINITION", "DEFINITION"),
  "atlasExposureReference" = c("SQL_DEFINITION", "DEFINITION")
)

#' @title
#' Export Reference tables
#' @description
#' Takes created reference tables (cohort definitions) from central rewardb and exports them to a zipped csv file
#' @param config global reward config
#' @param exportPath path to export files to before zipping
#' @param exportZipPath resulting zip files
exportReferenceTables <- function(
  config,
  exportPath = tempdir(),
  exportZipFile = "rewardb-references.zip"
) {
  scipen <- getOption("scipen")
  options(scipen = 999)
  connection <- DatabaseConnector::connect(connectionDetails = config$connectionDetails)
  on.exit(DatabaseConnector::disconnect(connection))

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
  options(scipen = scipen)
}

#' @title
#' Import reference tables
#' @description
#' Note that this always overwrites the existing reference tables stored in the database
#' @param cdmConfig cdmConfig object
#' @param zipFilePath zip file path
#' @param usePgCopy Use postgre copy function (not included in database connector)
importReferenceTables <- function(cdmConfig, zipFilePath, usePgCopy = FALSE) {
  unzipAndVerify(zipFilePath, cdmConfig$referencePath, TRUE)
  connection <- DatabaseConnector::connect(connectionDetails = cdmConfig$connectionDetails)
  on.exit(DatabaseConnector::disconnect(connection))

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
      atlas_outcome_concept = cdmConfig$tables$atlasOutcomeConcept,
      atlas_exposure_reference = cdmConfig$tables$atlasExposureReference,
      atlas_exposure_concept = cdmConfig$tables$atlasExposureConcept,
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