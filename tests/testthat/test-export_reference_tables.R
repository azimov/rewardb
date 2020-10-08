# Title     : Test export zip file functions
# Objective : To test the construction of the reward-b postgres database and ensure that data are there
# Created by: James Gilbert
# Created on: 2020-10-05


configFilePath <- system.file("tests", "test.cfg.yml", package = "rewardb")
config <- yaml::read_yaml(configFilePath)
connection <- DatabaseConnector::connect(config$rewardbDatabase)

# Set up a database with constructed cohorts etc
rewardb::buildPgDatabase(configFilePath = configFilePath)
cohortDefintion <- RJSONIO::fromJSON(system.file("tests", "atlasCohort12047.json", package = "rewardb"))
sqlDefinition <- readr::read_lines(system.file("tests", "atlasCohort12047.sql", package = "rewardb"))
rewardb::insertAtlasCohortRef(connection, config, 12047, .cohortDefinition=cohortDefinition, .sqlDefinition=sqlDefinition)
conceptSetId <- 11933
conceptSetDefintion <- RJSONIO::fromJSON(system.file("tests", "conceptSet11933.json", package = "rewardb"))
rewardb::insertCustomExposureRef(connection, config, conceptSetId, "Test Exposure Cohort", conceptSetDefinition = conceptSetDefinition)


test_that("Export reference zip file", {
  unlink("rewardb-references.zip")
  unlink("reference_test_folder")
  rewardb::exportReferenceTables(config)
  expect_true(checkmate::checkFileExists("rewardb-references.zip"))
  rewardb::unzipAndVerify("rewardb-references.zip", "reference_test_folder", TRUE)

  files <- file.path("reference_test_folder", paste0(rewardb::CONST_REFERENCE_TABLES, ".csv"))
  for(file in files) {
    expect_true(checkmate::checkFileExists(file))
  }
})

# Check that the vocabulary schema is there

# Check that creation of a CEM sumamry table works
DatabaseConnector::disconnect(connection)