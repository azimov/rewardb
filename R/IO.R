CONST_META_FILE_NAME <- "rb-meta.json"
#'
#' @description
#' Used to unzip and check all files in a zip folder with meta data file containing md5 hashes at time of creation
#' Used by both results generation and reference files
#'
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


pgCopy <- function(connectionDetails, csvFile, schema, tableName, sep = ",") {
  checkmate::assert(connectionDetails$dbms == "postgresql")
  passwordCommand <- paste0("PGPASSWORD=", connectionDetails$password)

  if (.Platform$OS.type == "windows") {

    winPsqlPath <- Sys.getenv("WIN_PSQL_PATH")
    passwordCommand <- paste0("$env:", passwordCommand, ";")
    command <- paste0('"', file.path(winPsqlPath, "psql.exe"), '"')

    if (!file.exists(command)) {
      stop("Error, could not find psql")
    }
  } else {
    command <- "psql"
  }
  # Read first line to get header column order, we assume these are large files
  head <- read.csv(file = csvFile, nrows = 1, sep=sep)
  headers <- stringi::stri_join(names(head), collapse = ", ")
  hostServerDb <- strsplit(connectionDetails$server, "/")[[1]]

  copyCommand <- paste(
    passwordCommand,
    command,
    "-h", hostServerDb[[1]],
    "-d", hostServerDb[[2]],
    "-p", connectionDetails$port,
    "-U", connectionDetails$user,
    "-c \"\\copy", paste0(schema, ".", tableName),
    paste0("(", headers, ")"),
    "FROM", paste0("'", csvFile, "'"),
    paste0("DELIMITER '", sep, "' CSV HEADER QUOTE E'\b';\"")
  )

  result <- base::system(copyCommand)
  if (result != 0) {
    stop("Copy failure, psql returned a non zero status")
  }
  ParallelLogger::logInfo(paste("Copy file complete", csvFile))
}

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