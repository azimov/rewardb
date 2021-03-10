# Title     : Test phenotype library
# Objective : Test that phenotype library can be added
# Created by: jamie
# Created on: 2020-10-21
test_that("Phenotype Library From github", {
  # Set up a database with constructed cohorts etc
  buildPgDatabase(configFilePath = configFilePath, buildPhenotypeLibrary = TRUE, generatePlSql = FALSE)
  qdf <- DatabaseConnector::renderTranslateQuerySql(
    connection,
    "SELECT count(*) as RESULTS_COUNT FROM @schema.atlas_outcome_reference",
    schema = config$rewardbResultsSchema
  )

  expect_true(qdf$RESULTS_COUNT[[1]] > 100)
})