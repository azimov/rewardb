# Title     : Test build postgres dataabse
# Objective : To test the construction of the reward-b postgres database and ensure that data are there
# Created by: James Gilbert
# Created on: 2020-10-05

test_that("build rewardb postgres db", {
  buildPgDatabase(configFilePath = configFilePath, recreateCem = TRUE)
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
})


test_that("Add and remove atlas cohort references", {

  cohortDefinition <- RJSONIO::fromJSON(file.path("testCfg", "atlasCohort12047.json"))
  sqlDefinition <- readr::read_file(file.path("testCfg", "atlasCohort12047.sql"))
  insertAtlasCohortRef(connection = connection, config = config, atlasId = 12047, cohortDefinition = cohortDefinition, sqlDefinition = sqlDefinition)

  qdf <- DatabaseConnector::renderTranslateQuerySql(
    connection,
    "SELECT  * FROM @schema.atlas_outcome_reference",
    schema = config$rewardbResultsSchema
  )
  expect_true(12047 %in% qdf$ATLAS_ID)

  removeAtlasCohort(connection, config, 12047)

  qdf <- DatabaseConnector::renderTranslateQuerySql(
    connection,
    "SELECT  * FROM @schema.atlas_outcome_reference",
    schema = config$rewardbResultsSchema
  )
  expect_false(12047 %in% qdf$ATLAS_ID)
})

test_that("Add and remove custom exposure references", {

  conceptSetId <- 11933
  conceptSetDefinition <- RJSONIO::fromJSON(file.path("testCfg", "conceptSet1.json"))
  insertCustomExposureRef(connection, config, conceptSetId, "Test Exposure Cohort", conceptSetDefinition = conceptSetDefinition)

  qdf <- DatabaseConnector::renderTranslateQuerySql(
    connection,
    "SELECT  * FROM @schema.custom_exposure",
    schema = config$rewardbResultsSchema
  )
  expect_true(conceptSetId %in% qdf$CONCEPT_SET_ID)

  qdf <- DatabaseConnector::renderTranslateQuerySql(
    connection,
    "SELECT * FROM @schema.cohort_definition WHERE cohort_definition_id
            IN ( SELECT cohort_definition_id FROM @schema.custom_exposure
              WHERE concept_set_id = @concept_set_id AND atlas_url = '@atlas_url');",
    schema = config$rewardbResultsSchema,
    concept_set_id = conceptSetId,
    atlas_url = config$webApiUrl
  )

  expect_true(nrow(qdf) > 0)

  removeCustomExposureCohort(connection, config, conceptSetId)

  qdf <- DatabaseConnector::renderTranslateQuerySql(
    connection,
    "SELECT * FROM @schema.cohort_definition WHERE cohort_definition_id
            IN ( SELECT cohort_definition_id FROM @schema.custom_exposure
              WHERE concept_set_id = @concept_set_id AND atlas_url = '@atlas_url');",
    schema = config$rewardbResultsSchema,
    concept_set_id = conceptSetId,
    atlas_url = config$webApiUrl
  )

  expect_true(nrow(qdf) == 0)

  qdf <- DatabaseConnector::renderTranslateQuerySql(
    connection,
    "SELECT  * FROM @schema.custom_exposure",
    schema = config$rewardbResultsSchema
  )
  expect_false(conceptSetId %in% qdf$CONCEPT_SET_ID)

})