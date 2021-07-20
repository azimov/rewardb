

test_that("Full data generation and export", {
  fullDbSetup()
  qdf <- DatabaseConnector::renderTranslateQuerySql(
    connection,
    "SELECT count(*) as cnt FROM @results_schema.data_source",
    results_schema = config$rewardbResultsSchema
  )
  expect_true(qdf$CNT[[1]] == 1)

  runDataBuild()

  # Assert that the tables contain d ata
  qdf <- DatabaseConnector::renderTranslateQuerySql(
    connection,
    "SELECT count(*) as results_count FROM @results_schema.scc_result WHERE ANALYSIS_ID = 1",
    results_schema = config$rewardbResultsSchema
  )

  expect_true(qdf$RESULTS_COUNT[[1]] > 0)
})