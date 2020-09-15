defaultDf <- function() {
  data.frame(
    col_a = c(1, 2, 3, 4)
  )
}

createCsv <- function(filename, path, data = defaultDf()) {
  filepath <- file.path(path, filename)
  write.csv(data, filepath, row.names = FALSE)
  return(filepath)
}

test_that("export works", {
  tmp <- tempdir()
  folder <- file.path(tmp, stringi::stri_rand_strings(1, 5))
  dir.create(folder)
  zipFilePath <- file.path(tmp, paste0(stringi::stri_rand_strings(1, 5), ".zip"))
  unzipPath <- file.path(tmp, stringi::stri_rand_strings(1, 5))

  # Create a csv file
  createCsv(filename = "test_file.csv", path = folder)
  config <- yaml::read_yaml(system.file("tests", "test.cfg.yml", package = "rewardb"))
  config$exportPath <- folder
  rewardb::exportResults(config, exportZipFile = zipFilePath)
  expect_true(checkmate::check_file_exists(zipFilePath))

  rewardb::unzipAndVerify(zipFilePath, unzipPath, TRUE)
  # cleanup
  unlink(folder, recursive = TRUE, force = TRUE)
  unlink(unzipPath)
})

test_that("bad checksums fail - modify zipped csv after function", {
  tmp <- tempdir()
  folder <- file.path(tmp, stringi::stri_rand_strings(1, 5))
  dir.create(folder)
  zipFilePath <- file.path(tmp, paste0(stringi::stri_rand_strings(1, 5), ".zip"))
  unzipPath <- file.path(tmp, stringi::stri_rand_strings(1, 5))

  # Create a csv file
  createCsv(filename = "test_file.csv", path = folder)
  config <- yaml::read_yaml(system.file("tests", "test.cfg.yml", package = "rewardb"))
  config$exportPath <- folder
  rewardb::exportResults(config, exportZipFile = zipFilePath)
  # Change the csv file inside the zip
  createCsv(filename = "test_file.csv", path = folder, data = data.frame(col_a = c(5, 6, 7, 8, 9)))
  zip::zipr_append(zipFilePath, file.path(folder, "test_file.csv"), include_directories = FALSE)

  expect_error(rewardb::unzipAndVerify(zipFilePath, unzipPath, TRUE))
  # cleanup
  unlink(folder, recursive = TRUE, force = TRUE)
  unlink(unzipPath)
})

createTestSchema <- function(connection) {
  schemaName <- paste0("test_schema_", stringi::stri_rand_strings(1, 5))
  DatabaseConnector::renderTranslateExecuteSql(connection, "CREATE SCHEMA @schema_name;", schema_name = schemaName)
  ParallelLogger::logInfo(paste("Created test schema", schemaName))
  return(schemaName)
}

dropTestSchema <- function(connection, schemaName) {
  ParallelLogger::logInfo(paste("Cleaning up schema", schemaName))
  DatabaseConnector::renderTranslateExecuteSql(connection, "DROP SCHEMA IF EXISTS @schema_name CASCADE;", schema_name = schemaName)
}

test_that("import copy functions", {
  config <- yaml::read_yaml(system.file("tests", "test.cfg.yml", package = "rewardb"))
  connection <- DatabaseConnector::connect(config$rewardbDatabase)
  schemaName <- createTestSchema(connection)
  tmp <- tempdir()
  folder <- file.path(tmp, stringi::stri_rand_strings(1, 5))
  dir.create(folder)
  zipFilePath <- file.path(tmp, paste0(stringi::stri_rand_strings(1, 5), ".zip"))
  unzipPath <- file.path(tmp, stringi::stri_rand_strings(1, 5))
  # Create a csv file
  createCsv(filename = "test_file.csv", path = folder)
  config$exportPath <- folder
  rewardb::exportResults(config, exportZipFile = zipFilePath)

  ParallelLogger::logInfo(paste("Testing insert", schemaName))
  rewardb::importResultsFiles(config$rewardbDatabase, schemaName, zipFilePath, unzipPath = unzipPath, overwrite = TRUE)

  results <- DatabaseConnector::renderTranslateQuerySql(
    connection = connection,
    sql = "SELECT * FROM @schema.test_file",
    schema = schemaName
  )

  expect_true(
    checkmate::checkDataFrame(
      results,
      min.cols = 1,
      max.cols = 1,
      min.rows = 4,
      max.rows = 4
    )
  )
  # cleanup
  unlink(folder, recursive = TRUE, force = TRUE)
  unlink(unzipPath)
  dropTestSchema(connection, schemaName)
  DatabaseConnector::disconnect(connection)
})