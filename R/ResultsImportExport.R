CONST_META_FILE_NAME <- "rb-meta.json"

exportResults <- function(config, exportZipFile = "rewardb-export.zip") {
  # Collect all files and make a hash
  meta <- list()
  meta$config <- config
  meta$hashList <- list()

  exportFiles <- Sys.glob(file.path(config$exportPath, "*.csv"))

  for (file in exportFiles) {
    meta$hashList[[basename(file)]] <- tools::md5sum(file)[[1]]
  }
  metaDataFilename <- file.path(config$exportPath, rewardb::CONST_META_FILE_NAME)
  jsonlite::write_json(meta, metaDataFilename)

  zip::zipr(exportZipFile, append(exportFiles, metaDataFilename), include_directories = FALSE)
}

unzipAndVerify <- function(exportZipFilePath, unzipPath, overwrite) {
  ParallelLogger::logInfo("Inflating zip archive")
  if (!dir.exists(unzipPath)) {
    dir.create(unzipPath)
  } else if (!overwrite) {
    stop(paste("Folder", unzipPath, "exists and overwite = FALSE "))
  }
  # Unzip full file
  utils::unzip(zipfile = exportZipFilePath, exdir = unzipPath, overwrite = TRUE)
  # Perform checksum verifications
  metaFilePath <- file.path(unzipPath, rewardb::CONST_META_FILE_NAME)
  checkmate::assert_file_exists(metaFilePath)
  meta <- jsonlite::read_json(file.path(unzipPath, rewardb::CONST_META_FILE_NAME))

  ParallelLogger::logInfo(paste("Verifying file checksums"))
  # Check files are valid

  for (file in names(meta$hashList)) {
    hash <- meta$hashList[[file]]
    ParallelLogger::logInfo(paste("checking file hash", file, hash))
    unzipFile <- file.path(unzipPath, file)
    checkmate::assert_file_exists(unzipFile)
    verifyCheckSum <- tools::md5sum(unzipFile)[[1]]
    ParallelLogger::logInfo(paste(hash, verifyCheckSum))
    checkmate::assert_true(hash == verifyCheckSum)
  }

  return(lapply(names(meta$hashList), function(file) { tools::file_path_as_absolute(file.path(unzipPath, file)) }))
}

importResults <- function(connectionDetails, resulstSchema, exportZipFilePath, unzipPath = "rb-import", overwrite = TRUE) {
  files <- unzipAndVerify(exportZipFilePath, unzipPath, overwrite)
  connection <- DatabaseConnector::connect(connectionDetails)
  # Bulk insert data in to tables
  tryCatch(
    {
      for (csvFile in files) {
        tableName <- stringr::str_split(basename(csvFile), ".csv")[[1]][[1]]
        data <- read.csv(csvFile)
        DatabaseConnector::insertTable(
          connection,
          paste0(resulstSchema, ".", tableName),
          data,
          dropTableIfExists = FALSE,
          createTable = TRUE,
          tempTable = FALSE,
          oracleTempSchema = NULL,
          useMppBulkLoad = FALSE,
          progressBar = TRUE,
          camelCaseToSnakeCase = FALSE
        )
      }
    },
    error = ParallelLogger::logError,
    finally = function() {
      DatabaseConnector::disconnect(connection)
    }
  )

}