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

.checkPsqlExists <- function(cmd, testCmd = "--version") {
  res <- base::system(paste(cmd, testCmd))
  if (res != 0) {
    stop("Error psql command did not retrun a 0 status. Copy util will not function")
  }
}

#' Only works with postgres
importResultsFiles <- function(
  connectionDetails,
  resultsSchema,
  exportZipFilePath,
  unzipPath = "rb-import",
  winPsqlPath = NULL,
  .checkTables = TRUE)
{
  checkmate::assert(connectionDetails$dbms == "postgresql")
  passwordCommand <- paste0("PGPASSWORD=", connectionDetails$password)
  if (.Platform$OS.type == "windows") {
    passwordCommand <- paste0("$env:", passwordCommand, ";")
    command <- file.path(winPsqlPath, "psql.exe")

    if (!file.exists(command)) {
      stop("Error, could not find psql")
    }
  } else {
    command <- "psql"
  }

  .checkPsqlExists(command)
  files <- unzipAndVerify(exportZipFilePath, unzipPath, TRUE)
  meta <- getMetaDt(unzipPath)
  hostServerDb <- strsplit(connectionDetails$server, "/")[[1]]

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
        # Read first line to get header column order, we assume these are large files
        head <- read.csv(file=csvFile, nrows=1)
        headers <- stringi::stri_join(names(head),collapse = ", ")

        copyCommand <- paste(
          passwordCommand,
          command,
          "-h", hostServerDb[[1]],
          "-d", hostServerDb[[2]],
          "-p", connectionDetails$port,
          "-U", connectionDetails$user,
          "-c \"\\copy", paste0(resultsSchema, ".", tableName),
          paste0("(", headers, ")"),
          "FROM", paste0("'", csvFile, "'"),
          "DELIMITER ',' CSV HEADER;\""
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