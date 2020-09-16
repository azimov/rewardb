CONST_META_FILE_NAME <- "rb-meta.json"

exportResults <- function(
  config,
  exportZipFile = "rewardb-export.zip",
  csvPattern = "*.csv",
  tableNames = list()
) {
  # Collect all files and make a hash
  meta <- list()
  meta$config <- config
  meta$hashList <- list()
  meta$tableList <- tableNames

  exportFiles <- Sys.glob(file.path(config$exportPath, csvPattern))

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

getMetaDt <- function(unzipPath) {
  jsonlite::read_json(file.path(unzipPath, rewardb::CONST_META_FILE_NAME))
}

.checkPsqlExists <- function(testCmd = "psql --version") {
  res <- base::system(testCmd)
  if (res != 0) {
    stop("Error psql command not found on system. Copy util will not function")
  }
}

importResultsFiles <- function(connectionDetails, resulstSchema, exportZipFilePath, unzipPath = "rb-import") {
  .checkPsqlExists()
  files <- unzipAndVerify(exportZipFilePath, unzipPath, TRUE)
  meta <- getMetaDt(unzipPath)
  hostServerDb <- strsplit(connectionDetails$server, "/")[[1]]
  connection <- DatabaseConnector::connect(connectionDetails)
  # Bulk insert data in to tables with pgcopy
  tryCatch(
    {
      tables <- DatabaseConnector::getTableNames(connection, resulstSchema)
      for (csvFile in files) {
        ParallelLogger::logInfo(paste("Uploading file", csvFile))

        if (basename(csvFile) %in% meta$tableList) {
          tableName <- meta$tableList[[basename(csvFile)]]
        } else {
          tableName <- stringr::str_split(basename(csvFile), ".csv")[[1]][[1]]
        }

        if (!(tableName %in% tables)) {
          ParallelLogger::logWarn(paste("Skipping table", tableName, "not found in schema"))
        }

        copyCommand <- paste(
          paste0("PGPASSWORD=", connectionDetails$password),
          "psql -h", hostServerDb[[1]],
          "-d", hostServerDb[[2]],
          "-p", connectionDetails$port,
          "-U", connectionDetails$user,
          "-c \"\\copy", paste0(resulstSchema, ".", tableName),
          "FROM", paste0("'", csvFile, "'"),
          "WITH CSV HEADER;\""
        )

        result <- base::system(copyCommand)
        if (result != 0) {
          stop("Copy failure, psql returned a non zero status")
        }
        ParallelLogger::logInfo(paste("Copy file complete", csvFile))
      }
    },
    error = ParallelLogger::logError,
    finally = function() {
      DatabaseConnector::disconnect(connection)
    }
  )

}

importResultsZip <- function(resultsZipPath, configFilePath="config/global-cfg.yml", unzipPath = "rb-import") {
  config <- yaml::read_yaml(configFilePath)
  rewardb::importResultsFiles(config$rewardbDatabase, config$rewardbResultsSchema, resultsZipPath, unzipPath = unzipPath)
}