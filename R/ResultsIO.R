#' Generic function that creates a zip file of csvs that will be imported in to the reward postgres instance
exportResults <- function(
  config,
  exportZipFile = "rewardb-export.zip",
  csvPattern = "*.csv",
  tableNames = list(),
  cdmVersion = NULL,
  databaseId = NULL,
  exportPath = NULL
) {
  # Collect all files and make a hash
  meta <- list()
  meta$hashList <- list()
  meta$tableNames <- tableNames
  meta$cdmVersion <- cdmVersion
  meta$databaseId <- databaseId

  if (is.null(exportPath)) {
    exportPath <- config$exportPath
  }

  exportFiles <- Sys.glob(file.path(exportPath, csvPattern))

  for (file in exportFiles) {
    meta$hashList[[basename(file)]] <- tools::md5sum(file)[[1]]
  }

  metaDataFilename <- file.path(exportPath, CONST_META_FILE_NAME)
  jsonlite::write_json(meta, metaDataFilename)

  zip::zipr(exportZipFile, append(exportFiles, metaDataFilename), include_directories = FALSE)

}

getMetaDt <- function(unzipPath) {
  jsonlite::read_json(file.path(unzipPath, CONST_META_FILE_NAME))
}

#' Only works with postgres
importResultsFiles <- function(
  connectionDetails,
  resultsSchema,
  exportZipFilePath,
  unzipPath = "rb-import",
  .checkTables = FALSE,
  .debug = FALSE
)
{
  ParallelLogger::logInfo("Importing results file")
  checkmate::assert(connectionDetails$dbms == "postgresql")
  files <- unzipAndVerify(exportZipFilePath, unzipPath, TRUE)
  meta <- getMetaDt(unzipPath)

  connection <- DatabaseConnector::connect(connectionDetails)
  on.exit(DatabaseConnector::disconnect(connection))
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

        pgCopy(connectionDetails, csvFile, resultsSchema, tableName, fileEncoding = "UTF-8-BOM", .echoCommand = .debug)
      }
    },
    error = ParallelLogger::logError
  )
}

importResultsZip <- function(resultsZipPath, configFilePath="config/global-cfg.yml", unzipPath = "rb-import") {
  config <- loadGlobalConfig(configFilePath)
  importResultsFiles(config$connectionDetails, config$rewardbResultsSchema, resultsZipPath, unzipPath = unzipPath)
}