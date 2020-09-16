test_that("create reference tables", {
  ParallelLogger::logInfo("Creating and populating reference tables only")

  rewardb::fullExecution(
    configFilePath = system.file("tests", "test.cfg.yml", package = "rewardb"),
    .createReferences = TRUE,
    .addDefaultAtlasCohorts = FALSE,
    .createExposureCohorts = FALSE,
    .createOutcomeCohorts = FALSE,
    .generateSummaryTables = FALSE,
    .runSCC = FALSE,
    dataSources = NULL,
  )

  expect_error(checkmate::assertFileExists("rewardb-export.zip"))
})

test_that("full data build", {

  rewardb::fullExecution(
    configFilePath = system.file("tests", "test.cfg.yml", package = "rewardb")
  )
  ParallelLogger::logInfo("Performing full execution")
  expect_true(checkmate::checkFileExists("rewardb-export.zip"))
  unlink("rewardb-export.zip")
})

test_that("data import", {
  rewardb::fullExecution(
    configFilePath = system.file("tests", "test.cfg.yml", package = "rewardb")
  )
  ParallelLogger::logInfo("Performing full execution")
  expect_true(checkmate::checkFileExists("rewardb-export.zip"))

  rewardb::buildPgDatabase(configFilePath = system.file("tests", "test.cfg.yml", package = "rewardb"))
  rewardb::importResultsZip(
    resultsZipPath = "rewardb-export.zip",
    configFilePath = system.file("tests", "test.cfg.yml", package = "rewardb")
  )

  unlink("rewardb-export.zip")

  config <- yaml::read_yaml(system.file("tests", "test.cfg.yml", package = "rewardb"))
  connection <- DatabaseConnector::connect(config$rewardbDatabase)

  # Test that data has been inserted ok
  res <- DatabaseConnector::renderTranslateQuerySql(
    connection,
    "SELECT count(*) FROM @schema.scc_result",
    rewardbResultsSchema = config$rewardb
  )
  expect_true(res > 0)
})