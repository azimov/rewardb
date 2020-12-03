CONST_REFERENCE_TABLES <- c(
  "concept_set_definition",
  "cohort_definition",
  "outcome_cohort_definition",
  "atlas_outcome_reference",
  "atlas_concept_reference",
  "custom_exposure",
  "custom_exposure_concept",
  "concept_set_definition",
  "outcome_cohort_definition"
)

#' Export Reference tables
#' @description
#' Takes created reference tables (cohort definitions) from central rewardb and exports them to a zipped csv file
exportReferenceTables <- function(
  config,
  exportPath = tempdir(),
  exportZipFile = "rewardb-references.zip"
) {
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
        write.csv(data, file, na = "", row.names = FALSE)
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
}

#'
#' @description
#' Note that this always overwrites the existing reference tables stored in the database
importReferenceTables <- function(cdmConfig, zipFilePath, refFolder, usePgCopy = FALSE) {
  unzipAndVerify(zipFilePath, refFolder, TRUE)
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
        custom_exposure_concept = cdmConfig$tables$customExposureConcept
      )

      fileList <- file.path(refFolder, paste0(rewardb::CONST_REFERENCE_TABLES, ".csv"))
      for (file in fileList) {
        snakeName <- SqlRender::snakeCaseToCamelCase(strsplit(basename(file), ".csv")[[1]])
        tableName <- cdmConfig$tables[[snakeName]]

        # TODO: Find a solution to uploading atlas cohort references faster than
        if (cdmConfig$connectionDetails$dbms == "postgresql" & usePgCopy & snakeName != "atlasOutcomeReference") {
          print(paste("Using pgcopy to upload", snakeName, tableName, file))
          pgCopy(connectionDetails = cdmConfig$connectionDetails, csvFileName = file, schema = cdmConfig$referenceSchema, tableName = tableName)
        } else {
          print(paste("Using db append table", snakeName, tableName, file))
          data <- read.csv(file)
          DatabaseConnector::insertTable(
            connection = connection,
            tableName = paste(cdmConfig$referenceSchema, tableName, sep = "."),
            data = data,
            progressBar = TRUE,
            dropTableIfExists = TRUE,
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