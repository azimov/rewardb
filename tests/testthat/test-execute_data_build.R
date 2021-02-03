configFilePath <- system.file("tests", "test.cfg.yml", package = "rewardb")
config <- loadGlobalConfig(configFilePath)
connection <- DatabaseConnector::connect(connectionDetails = config$connectionDetails)

# Set up a database with constructed cohorts etc
buildPgDatabase(configFilePath = configFilePath, buildPhenotypeLibrary = FALSE)
# Add some extra scc settings
addAnalysisSettingsJson(connection, config, settingsFilePath = system.file("tests", "testSCCSettings.json", package = "rewardb"))

cohortDefinition <- RJSONIO::fromJSON(system.file("tests", "atlasCohort12047.json", package = "rewardb"))
sqlDefinition <- readr::read_file(system.file("tests", "atlasCohort12047.sql", package = "rewardb"))
insertAtlasCohortRef(connection, config, 12047, cohortDefinition = cohortDefinition, sqlDefinition = sqlDefinition)

cohortDefinition <- RJSONIO::fromJSON(system.file("tests", "atlasExposureCohort19321.json", package = "rewardb"))
sqlDefinition <- readr::read_file(system.file("tests", "atlasCohort19321.sql", package = "rewardb"))
insertAtlasCohortRef(connection, config, 19321, cohortDefinition = cohortDefinition, sqlDefinition = sqlDefinition, exposure = TRUE)

conceptSetId <- 11933
conceptSetDefinition <- RJSONIO::fromJSON(system.file("tests", "conceptSet1.json", package = "rewardb"))
insertCustomExposureRef(connection, config, conceptSetId, "Test Exposure Cohort", conceptSetDefinition = conceptSetDefinition)

cdmConfigPath <- system.file("tests", "eunomia.cdm.cfg.yml", package = "rewardb")
cdmConfig <- loadCdmConfig(cdmConfigPath)

zipFilePath <- "rewardb-references.zip"
refFolder <- "reference_test_folder"
unlink(zipFilePath)
unlink(refFolder)
exportReferenceTables(config)
importReferenceTables(cdmConfig, zipFilePath)


unlink("export")
unlink("rb-import")

test_that("Full data generation and export", {
  registerCdm(connection, config, cdmConfig)

  qdf <- DatabaseConnector::renderTranslateQuerySql(
    connection,
    "SELECT count(*) as cnt FROM @results_schema.data_source",
    results_schema = config$rewardbResultsSchema
  )
  expect_true(qdf$CNT[[1]] == 1)

  generateSccResults(cdmConfigPath, .getDbId = FALSE)
  importResultsFiles(config$connectionDetails, "test", "reward-b-scc-results-aid-1.zip", .debug = TRUE)
  importResultsFiles(config$connectionDetails, "test", "reward-b-scc-results-aid-2.zip", .debug = TRUE)

  # Assert that the tables contain data
  qdf <- DatabaseConnector::renderTranslateQuerySql(
    connection,
    "SELECT count(*) as results_count FROM @results_schema.scc_result WHERE ANALYSIS_ID = 1",
    results_schema = config$rewardbResultsSchema
  )

  expect_true(qdf$RESULTS_COUNT[[1]] > 0)

  qdf <- DatabaseConnector::renderTranslateQuerySql(
    connection,
    "SELECT count(*) as results_count FROM @results_schema.scc_result WHERE ANALYSIS_ID = 2",
    results_schema = config$rewardbResultsSchema
  )

  expect_true(qdf$RESULTS_COUNT[[1]] > 0)

  qdf <- DatabaseConnector::renderTranslateQuerySql(
    connection,
    "SELECT count(*) as results_count FROM @results_schema.time_on_treatment WHERE ANALYSIS_ID = 1",
    results_schema = config$rewardbResultsSchema
  )
  # Outcome cohorts should be created
  expect_true(qdf$RESULTS_COUNT[[1]] > 0)

  qdf <- DatabaseConnector::renderTranslateQuerySql(
    connection,
    "SELECT count(*) as results_count FROM @results_schema.time_on_treatment WHERE ANALYSIS_ID = 2",
    results_schema = config$rewardbResultsSchema
  )
  # Outcome cohorts should be created
  expect_true(qdf$RESULTS_COUNT[[1]] > 0)

})
DatabaseConnector::disconnect(connection)
unlink("export")
unlink("rb-import")