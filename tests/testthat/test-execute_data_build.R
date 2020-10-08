configFilePath <- system.file("tests", "test.cfg.yml", package = "rewardb")
config <- yaml::read_yaml(configFilePath)
connection <- DatabaseConnector::connect(config$rewardbDatabase)

# Set up a database with constructed cohorts etc
rewardb::buildPgDatabase(configFilePath = configFilePath)
cohortDefintion <- RJSONIO::fromJSON(system.file("tests", "atlasCohort12047.json", package = "rewardb"))
sqlDefinition <- readr::read_lines(system.file("tests", "atlasCohort12047.sql", package = "rewardb"))
rewardb::insertAtlasCohortRef(connection, config, 12047, cohortDefinition = cohortDefinition, sqlDefinition = sqlDefinition)
conceptSetId <- 11933
conceptSetDefintion <- RJSONIO::fromJSON(system.file("tests", "conceptSet1.json", package = "rewardb"))
rewardb::insertCustomExposureRef(connection, config, conceptSetId, "Test Exposure Cohort", conceptSetDefinition = conceptSetDefinition)


cdmConfig <- yaml::read_yaml(system.file("tests", "eunomia.cdm.cfg.yml", package = "rewardb"))

zipFilePath <- "rewardb-references.zip"
refFolder <- "reference_test_folder"
unlink(zipFilePath)
unlink(refFolder)
exportReferenceTables(config)
importReferenceTables(cdmConfig, zipFilePath, refFolder)


test_that("Full data generation on CDM", {
  ParallelLogger::logInfo("Performing full execution")
  rewardb::fullExecution(
    configFilePath = yaml::read_yaml(system.file("tests", "eunomia.cdm.cfg.yml", package = "rewardb"))
  )

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