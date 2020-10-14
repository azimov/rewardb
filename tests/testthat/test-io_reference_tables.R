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
sqlDefinition <- readr::read_file(system.file("tests", "atlasCohort12047.sql", package = "rewardb"))
rewardb::insertAtlasCohortRef(connection, config, 12047, cohortDefinition = cohortDefinition, sqlDefinition = sqlDefinition)
conceptSetId <- 11933
conceptSetDefintion <- RJSONIO::fromJSON(system.file("tests", "conceptSet1.json", package = "rewardb"))
rewardb::insertCustomExposureRef(connection, config, conceptSetId, "Test Exposure Cohort", conceptSetDefinition = conceptSetDefinition)


cdmConfig <- yaml::read_yaml(system.file("tests", "eunomia.cdm.cfg.yml", package = "rewardb"))

zipFilePath <- "rewardb-references.zip"
refFolder <- "reference_test_folder"
unlink(zipFilePath)
unlink(refFolder)

test_that("Export/Import reference zip file", {

  rewardb::exportReferenceTables(config)
  expect_true(checkmate::checkFileExists(zipFilePath))
  rewardb::unzipAndVerify(zipFilePath, refFolder, TRUE)

  files <- file.path(refFolder, paste0(rewardb::CONST_REFERENCE_TABLES, ".csv"))
  for (file in files) {
    expect_true(checkmate::checkFileExists(file))
  }

  importReferenceTables(cdmConfig, zipFilePath, refFolder)

  # Verify the tables existinces
  for (table in rewardb::CONST_REFERENCE_TABLES) {
    testSql <- "SELECT count(*) as tbl_count FROM @schema.@table"
    resp <- DatabaseConnector::renderTranslateQuerySql(connection, testSql, schema=cdmConfig$referenceSchema, table=table)
    expect_true(nrow(resp) > 0)
    expect_true(resp$TBL_COUNT[[1]] > 0)
  }

})

# Check that the vocabulary schema is there

# Check that creation of a CEM sumamry table works
DatabaseConnector::disconnect(connection)