# Title     : Test phenotype library
# Objective : Test that phenotype library can be added
# Created by: jamie
# Created on: 2020-10-21

configFilePath <- system.file("tests", "test.cfg.yml", package = "rewardb")
config <- loadGlobalConfig(configFilePath)
connection <- DatabaseConnector::connect(connectionDetails = config$connectionDetails)

# Set up a database with constructed cohorts etc
buildPgDatabase(configFilePath = configFilePath)

test_that("Phenotype Library From github", {
  addPhenotypeLibrary(connection, config)

  qdf <- DatabaseConnector::renderTranslateQuerySql(
    connection,
    "SELECT count(*) as RESULTS_COUNT FROM @schema.atlas_outcome_reference",
    schema = config$rewardbResultsSchema
  )

  expect_true(qdf$RESULTS_COUNT[[1]] > 100)
})