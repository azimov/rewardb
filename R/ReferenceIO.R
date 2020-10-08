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

  connection <- DatabaseConnector::connect(config$rewardbDatabase)
  # Collect all files and make a hash
  meta <- list()
  meta$config <- config
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
    readr::write_excel_csv(data, file, na = "")
    meta$hashList[[basename(file)]] <- tools::md5sum(file)[[1]]
  }

  DatabaseConnector::disconnect(connection)

  metaDataFilename <- file.path(config$exportPath, rewardb::CONST_META_FILE_NAME)
  jsonlite::write_json(meta, metaDataFilename)

  exportFiles <- file.path(exportPath, paste0(rewardb::CONST_REFERENCE_TABLES, ".csv"))
  zip::zipr(exportZipFile, append(exportFiles, metaDataFilename), include_directories = FALSE)

  ParallelLogger::logInfo(paste("Created export zipfile", exportZipFile))
}

#'
#' @description
#' Note that this always overwrites the existing reference tables stored in the database
importReferenceTables <- function(cdmConfig, zipFilePath, refFolder) {
  unzipAndVerify(zipFilePath, refFolder, TRUE)
  connection <- DatabaseConnector::connect(connectionDetails = cdmConfig$connectionDetails)

  fileList <- file.path(refFolder, paste0(rewardb::CONST_REFERENCE_TABLES, ".csv"))
  for (file in fileList) {
    data <- read.csv(file)
    tableName <- strsplit(basename(file), ".csv")[[1]]
    DatabaseConnector::insertTable(
      connection,
      tableName = paste0(cdmConfig$referenceSchema, ".", tableName),
      data = data,
      progressBar = TRUE,
      dropTableIfExists = TRUE,
      createTable = TRUE
    )
  }
  DatabaseConnector::disconnect(connection)
}