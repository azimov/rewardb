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
  rewardb::fullExecution(configFilePath = system.file("tests", "test.cfg.yml", package = "rewardb"))
  ParallelLogger::logInfo("Performing full execution")
  expect_true(checkmate::checkFileExists("rewardb-export.zip"))
})

