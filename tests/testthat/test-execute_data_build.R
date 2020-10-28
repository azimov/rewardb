configFilePath <- system.file("tests", "test.cfg.yml", package = "rewardb")
config <- loadGlobalConfig(configFilePath)
connection <- DatabaseConnector::connect(config$connectionDetails)

# Set up a database with constructed cohorts etc
rewardb::buildPgDatabase(configFilePath = configFilePath)
cohortDefinition <- RJSONIO::fromJSON(system.file("tests", "atlasCohort12047.json", package = "rewardb"))
sqlDefinition <- readr::read_file(system.file("tests", "atlasCohort12047.sql", package = "rewardb"))
rewardb::insertAtlasCohortRef(connection, config, 12047, cohortDefinition = cohortDefinition, sqlDefinition = sqlDefinition)
conceptSetId <- 11933
conceptSetDefinition <- RJSONIO::fromJSON(system.file("tests", "conceptSet1.json", package = "rewardb"))
rewardb::insertCustomExposureRef(connection, config, conceptSetId, "Test Exposure Cohort", conceptSetDefinition = conceptSetDefinition)

cdmConfigPath <- system.file("tests", "eunomia.cdm.cfg.yml", package = "rewardb")
cdmConfig <- yaml::read_yaml(cdmConfigPath)

zipFilePath <- "rewardb-references.zip"
refFolder <- "reference_test_folder"
unlink(zipFilePath)
unlink(refFolder)
exportReferenceTables(config)
importReferenceTables(cdmConfig, zipFilePath, refFolder)


unlink("export")
unlink("rb-import")

test_that("Full data generation and export", {
  generateSccResults(cdmConfigPath)
  importResultsFiles(config$connectionDetails, "test", "rewardb-export.zip")

  # Assert that the tables contain data
  qdf <- DatabaseConnector::renderTranslateQuerySql(
    connection,
    "SELECT count(*) as results_count FROM @results_schema.scc_result",
    results_schema = config$rewardbResultsSchema,
    cohort_table = cdmConfig$tables$outcomeCohort
  )
  # Outcome cohorts should be created
  expect_true(qdf$RESULTS_COUNT[[1]] > 0)
})

unlink("export")
unlink("rb-import")