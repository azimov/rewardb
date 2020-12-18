CONST_REFERENCE_TABLES <- c(
  "concept_set_definition",
  "cohort_definition",
  "outcome_cohort_definition",
  "atlas_outcome_reference",
  "atlas_concept_reference",
  "custom_exposure",
  "custom_exposure_concept",
  "concept_set_definition",
  "outcome_cohort_definition",
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
  scipen = getOption("scipen")
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
      suppressWarnings({write.csv(data, file, na = "", row.names = FALSE, fileEncoding = "ascii")})
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
        print(paste("Using pgcopy to upload", camelName, tableName, file))
        pgCopy(connectionDetails = cdmConfig$connectionDetails, csvFileName = file, schema = cdmConfig$referenceSchema, tableName = tableName)
      } else {
        print(paste("Using insert table", camelName, tableName, file))
        data <- read.csv(file)

        # Remove columns we don't want to store on the CDM
        if (camelName %in% names(rewardb::CONST_EXCLUDE_REF_COLS)) {
          data <- data[, !(names(data) %in% rewardb::CONST_EXCLUDE_REF_COLS[[camelName]])]
          print(names(data))
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
  options(scipen = scipen)
}