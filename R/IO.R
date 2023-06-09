#' @export
CONST_META_FILE_NAME <- "rb-meta.json"

#' @title
#' Unzip and verify results zip with meta-data json
#' @description
#' Used to unzip and check all files in a zip folder with meta data file containing md5 hashes at time of creation
#' Used by both results generation and reference files
#' @param exportZipFilePath zip file to inflate
#' @param unzipPath path to create
#' @param overwrite overwrite any existing
#' @export
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
  metaFilePath <- file.path(unzipPath, CONST_META_FILE_NAME)
  checkmate::assert_file_exists(metaFilePath)
  meta <- jsonlite::read_json(file.path(unzipPath, CONST_META_FILE_NAME))

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

.checkPsqlExists <- function(cmd, testCmd = "--version") {
  res <- base::system(paste(cmd, testCmd))
  if (res != 0) {
    stop("Error psql command did not retrun a 0 status. Copy util will not function")
  }
}

#' @importFrom vroom vroom_write
#' @export
pgCopyDataFrame <- function(connectionDetails, data, schema, tableName, .echoCommand = FALSE) {

  if (!length(data)) {
    warning("Cannot upload empty data frame")
    return(NULL)
  }

  csvFileName <- tempfile(fileext = "csv")

  dt <- do.call(data.frame, lapply(data, function(x) replace(x, is.infinite(x) | is.nan(x), NA)))
  scipen <- getOption("scipen")
  options(scipen = 999)
  on.exit({
    unlink(csvFileName)
    options(scipen = scipen)
  }, add = TRUE)

  vroom::vroom_write(dt, csvFileName, na = "", delim = ",")
  pgCopy(connectionDetails = connectionDetails, csvFileName = csvFileName, schema = schema, tableName = tableName, .echoCommand = .echoCommand)
}

#' @export
pgCopy <- function(connectionDetails,
                   csvFileName,
                   schema,
                   tableName,
                   sep = ",",
                   fileEncoding = "UTF-8",
                   renameHeadings = NULL,
                   .echoCommand = FALSE) {
  startTime <- Sys.time()

  # For backwards compatibility with older versions of DatabaseConnector
  if (is(connectionDetails$server, "function")) {
    hostServerDb <- strsplit(connectionDetails$server(), "/")[[1]]
    port <- connectionDetails$port()
    user <- connectionDetails$user()
    password <- connectionDetails$password()
  } else {
    hostServerDb <- strsplit(connectionDetails$server, "/")[[1]]
    port <- connectionDetails$port
    user <- connectionDetails$user
    password <- connectionDetails$password
  }

  # Required by psql:
  Sys.setenv("PGPASSWORD" = password)
  rm(password)
  on.exit(Sys.unsetenv("PGPASSWORD"))

  if (.Platform$OS.type == "windows") {
    winPsqlPath <- Sys.getenv("WIN_PSQL_PATH")
    command <- file.path(winPsqlPath, "psql.exe")
    if (!file.exists(command)) {
      stop("Could not find psql.exe in ", winPsqlPath)
    }
    command <- paste0("\"", command, "\"")
  } else {
    command <- "psql"
  }

  .checkPsqlExists(command)

  head <- read.csv(file = csvFileName, nrows = 1, sep = sep, fileEncoding = fileEncoding)

  if (!is.null(renameHeadings)) {
    head <- head %>% dplyr::rename(renameHeadings)
  }

  headers <- paste(names(head), collapse = ", ")
  headers <- paste0("(", headers, ")")
  tablePath <- paste(schema, tableName, sep = ".")
  filePathStr <- paste0("'", csvFileName, "'")

  if (is.null(port)) {
    port <- 5432
  }

  copyCommand <- paste(command,
                       "-h", hostServerDb[[1]], # Host
                       "-d", hostServerDb[[2]], # Database
                       "-p", port,
                       "-U", user,
                       "-c \"\\copy", tablePath,
                       headers,
                       "FROM", filePathStr,
                       paste0("DELIMITER '", sep, "' CSV HEADER QUOTE E'\b' NULL AS '' ;\""))

  if (.echoCommand) {
    ParallelLogger::logDebug(copyCommand)
  }

  result <- base::system(copyCommand)

  if (result != 0) {
    stop("Error while bulk uploading data, psql returned a non zero status. Status = ", result)
  }
  delta <- Sys.time() - startTime
  writeLines(paste("Uploading data took", signif(delta, 3), attr(delta, "units")))
}

#' @export
importVocabularyZip <- function(connectionDetails, vocabularyPath, vocabularySchema = "vocabulary") {
  unzipPath <- tempfile()
  dir.create(unzipPath)
  message("Unzipping file")
  utils::unzip(zipfile = vocabularyPath, exdir = unzipPath, overwrite = TRUE)
  for (csvFile in Sys.glob(file.path(unzipPath, "*.csv"))) {
    message(paste("Copying", csvFile))
    tableName <- strsplit(basename(csvFile), ".csv")[[1]]
    pgCopy(connectionDetails, csvFile, vocabularySchema, tableName, sep = "\t")
  }
  #unlink(unzipPath)
}