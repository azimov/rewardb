# Title     : Test build postgres dataabse
# Objective : To test the construction of the reward-b postgres database and ensure that data are there
# Created by: James Gilbert
# Created on: 2020-10-05


configFilePath <- system.file("tests", "test.cfg.yml", package = "rewardb")
config <- yaml::read_yaml(configFilePath)
connection <- DatabaseConnector::connect(config$rewardbDatabase)

test_that("build db", {
  rewardb::buildPgDatabase(configFilePath = configFilePath)
  qdf <- DatabaseConnector::renderTranslateQuerySql(
    connection,
    "SELECT  * FROM @schema.scc_result",
    schema = config$rewardbResultsSchema
  )
  expect_true(checkmate::check_data_frame(qdf))
  # Check that columns exist
  colnames <- c("SOURCE_ID", "OUTCOME_COHORT_ID", "TARGET_COHORT_ID", "RR")
  for (n in colnames) {
    expect_true(n %in% names(qdf))
  }

  rewardb::importCemSummary(system.file("tests", "matrix_summary.csv", package = "rewardb") , configFilePath = configFilePath)
})

# Check that the vocabulary schema is there

# Check that creation of a CEM sumamry table works

DatabaseConnector::disconnect(connection)