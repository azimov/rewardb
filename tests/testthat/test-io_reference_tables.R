# Title     : Test export zip file functions
# Objective : To test the construction of the reward-b postgres database and ensure that data are there
# Created by: James Gilbert
# Created on: 2020-10-05


configFilePath <- system.file("tests", "test.cfg.yml", package = "rewardb")
config <- loadGlobalConfig(configFilePath)
connection <- DatabaseConnector::connect(connectionDetails = config$connectionDetails)

# Set up a database with constructed cohorts etc
buildPgDatabase(configFilePath = configFilePath, buildPhenotypeLibrary = FALSE)
cohortDefinition <- RJSONIO::fromJSON(system.file("tests", "atlasCohort12047.json", package = "rewardb"))
sqlDefinition <- readr::read_file(system.file("tests", "atlasCohort12047.sql", package = "rewardb"))
insertAtlasCohortRef(connection, config, 12047, cohortDefinition = cohortDefinition, sqlDefinition = sqlDefinition)

cohortDefinition <- RJSONIO::fromJSON(system.file("tests", "atlasExposureCohort19321.json", package = "rewardb"))
sqlDefinition <- readr::read_file(system.file("tests", "atlasCohort19321.sql", package = "rewardb"))
insertAtlasCohortRef(connection, config, 19321, cohortDefinition = cohortDefinition, sqlDefinition = sqlDefinition, exposure = TRUE)

conceptSetId <- 11933
conceptSetDefinition <- RJSONIO::fromJSON(system.file("tests", "conceptSet1.json", package = "rewardb"))
insertCustomExposureRef(connection, config, conceptSetId, "Test Exposure Cohort", conceptSetDefinition = conceptSetDefinition)

cdmConfig <- loadCdmConfig(system.file("tests", "eunomia.cdm.cfg.yml", package = "rewardb"))

registerCdm(connection, config, cdmConfig)

DatabaseConnector::renderTranslateExecuteSql(connection,"DROP SCHEMA @schema CASCADE;", schema = cdmConfig$resultSchema)
DatabaseConnector::renderTranslateExecuteSql(connection,"CREATE SCHEMA @schema", schema = cdmConfig$resultSchema)

zipFilePath <- "rewardb-references.zip"
unlink(zipFilePath)
unlink(cdmConfig$referenceFolder)

test_that("Export/Import reference zip file", {

  exportReferenceTables(config)
  expect_true(checkmate::checkFileExists(zipFilePath))

  importReferenceTables(cdmConfig, zipFilePath)

  # Verify the tables existinces
  for (table in rewardb::CONST_REFERENCE_TABLES) {
    table <- cdmConfig$tables[[SqlRender::snakeCaseToCamelCase(table)]]
    testSql <- "SELECT count(*) as tbl_count FROM @schema.@table"
    resp <- DatabaseConnector::renderTranslateQuerySql(connection, testSql, schema=cdmConfig$referenceSchema, table=table)
    expect_true(nrow(resp) > 0)
    expect_true(resp$TBL_COUNT[[1]] > 0)
  }

  for (camelName in names(rewardb::CONST_EXCLUDE_REF_COLS)) {
    table <- cdmConfig$tables[[camelName]]
    testSql <- "SELECT count(*) as tbl_count FROM @schema.@table"
    resp <- DatabaseConnector::renderTranslateQuerySql(connection, testSql, schema=cdmConfig$referenceSchema, table=table)

    for(col in rewardb::CONST_EXCLUDE_REF_COLS[[camelName]]) {
      expect_false(col %in% names(resp))
    }
  }

})
# Reset schema to know that its beign updated properly
DatabaseConnector::renderTranslateExecuteSql(connection,"DROP SCHEMA @schema CASCADE;", schema = cdmConfig$resultSchema)
DatabaseConnector::renderTranslateExecuteSql(connection,"CREATE SCHEMA @schema", schema = cdmConfig$resultSchema)

test_that("Export/Import reference zip file with pgcopy", {

  importReferenceTables(cdmConfig, zipFilePath, usePgCopy = TRUE)

  # Verify the tables existinces
  for (table in rewardb::CONST_REFERENCE_TABLES) {
    table <- cdmConfig$tables[[SqlRender::snakeCaseToCamelCase(table)]]
    testSql <- "SELECT count(*) as tbl_count FROM @schema.@table"
    resp <- DatabaseConnector::renderTranslateQuerySql(connection, testSql, schema=cdmConfig$referenceSchema, table=table)
    expect_true(nrow(resp) > 0)
    expect_true(resp$TBL_COUNT[[1]] > 0)
  }

})
unlink(zipFilePath)
unlink(cdmConfig$referenceFolder)
# Check that creation of a CEM sumamry table works
DatabaseConnector::disconnect(connection)