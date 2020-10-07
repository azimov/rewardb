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

  qdf <- DatabaseConnector::renderTranslateQuerySql(
    connection,
    "SELECT count(*) as cohort_count FROM @schema.cohort_definition",
    schema = config$rewardbResultsSchema
  )
  # Cohorts should be created
  expect_true(qdf$COHORT_COUNT[[1]] > 0)

  qdf <- DatabaseConnector::renderTranslateQuerySql(
    connection,
    "SELECT count(*) as cohort_count FROM @schema.outcome_cohort_definition",
    schema = config$rewardbResultsSchema
  )
  # Cohorts should be created
  expect_true(qdf$COHORT_COUNT[[1]] > 0)

  rewardb::importCemSummary(system.file("tests", "matrix_summary.csv", package = "rewardb") , configFilePath = configFilePath)
})


test_that("Add atlas cohorts", {
  rewardb::insertAtlasCohortRef(connection, config, 12047)

  qdf <- DatabaseConnector::renderTranslateQuerySql(
    connection,
    "SELECT  * FROM @schema.atlas_reference_table",
    schema = config$rewardbResultsSchema
  )

  expect_true(nrow(qdf) > 0)
})

# Check that the vocabulary schema is there

# Check that creation of a CEM sumamry table works

DatabaseConnector::disconnect(connection)