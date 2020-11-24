exportResults <- function(
  config,
  exportZipFile = "rewardb-export.zip",
  csvPattern = "*.csv",
  tableNames = list()
) {
  # Collect all files and make a hash
  meta <- list()
  meta$hashList <- list()
  meta$tableNames <- tableNames

  exportFiles <- Sys.glob(file.path(config$exportPath, csvPattern))

  for (file in exportFiles) {
    meta$hashList[[basename(file)]] <- tools::md5sum(file)[[1]]
  }

  metaDataFilename <- file.path(config$exportPath, rewardb::CONST_META_FILE_NAME)
  jsonlite::write_json(meta, metaDataFilename)

  zip::zipr(exportZipFile, append(exportFiles, metaDataFilename), include_directories = FALSE)
}

getMetaDt <- function(unzipPath) {
  jsonlite::read_json(file.path(unzipPath, rewardb::CONST_META_FILE_NAME))
}

#' Only works with postgres
importResultsFiles <- function(
  connectionDetails,
  resultsSchema,
  exportZipFilePath,
  unzipPath = "rb-import",
  .checkTables = TRUE)
{
  checkmate::assert(connectionDetails$dbms == "postgresql")
  files <- unzipAndVerify(exportZipFilePath, unzipPath, TRUE)
  meta <- getMetaDt(unzipPath)

  connection <- DatabaseConnector::connect(connectionDetails)
  # Bulk insert data in to tables with pgcopy
  tryCatch(
    {
      tables <- DatabaseConnector::getTableNames(connection, resultsSchema)
      for (csvFile in files) {
        ParallelLogger::logInfo(paste("Uploading file", csvFile))
        tableName <- meta$tableNames[[basename(csvFile)]]
        if (is.null(tableName)) {
          tableName <- stringr::str_split(basename(csvFile), ".csv")[[1]][[1]]
        }

        if (.checkTables & !(stringr::str_to_upper(tableName) %in% tables)) {
          ParallelLogger::logWarn(paste("Skipping table", tableName, "not found in schema", tables))
          next
        }

        pgCopy(connectionDetails, csvFile, resultsSchema, tableName)
      }
    },
    error = ParallelLogger::logError
  )
  DatabaseConnector::disconnect(connection)
}

importResultsZip <- function(resultsZipPath, configFilePath="config/global-cfg.yml", unzipPath = "rb-import") {
  config <- loadGlobalConfig(configFilePath)
  rewardb::importResultsFiles(config$connectionDetails, config$rewardbResultsSchema, resultsZipPath, unzipPath = unzipPath)
}