test_that("Full data generation on CDM", {
  rewardb::buildPgDatabase(configFilePath = system.file("tests", "test.cfg.yml", package = "rewardb"))
  # Export references

  rewardb::fullExecution(
    configFilePath = system.file("tests", "test.cfg.yml", package = "rewardb")
  )
  ParallelLogger::logInfo("Performing full execution")
  expect_true(checkmate::checkFileExists("rewardb-export.zip"))

  rewardb::importResultsZip(
    resultsZipPath = "rewardb-export.zip",
    configFilePath = system.file("tests", "test.cfg.yml", package = "rewardb")
  )

  config <- yaml::read_yaml(system.file("tests", "test.cfg.yml", package = "rewardb"))
  connection <- DatabaseConnector::connect(config$rewardbDatabase)

  # Test that data has been inserted ok
  res <- DatabaseConnector::renderTranslateQuerySql(
    connection,
    "SELECT count(*) FROM @schema.scc_result",
    schema = config$rewardbResultsSchema
  )
  ParallelLogger::logInfo(paste("Found", res, "entries"))
  expect_true(res > 0)
  DatabaseConnector::disconnect(connection)
  unlink("rewardb-export.zip")
  unlink("rb-import")
})