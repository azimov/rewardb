# Set up a database with constructed cohorts etc
fullDbSetup()
runDataBuild()

test_that("Dashboard creation works", {
  Sys.setenv("REWARD_B_PASSWORD" = "postgres")
  buildDashboardFromConfig(appContextFile, configFilePath, performCalibration = TRUE)
})

appContext <- loadShinyAppContext(appContextFile, configFilePath)
test_that("Data model utilitiy queries", {
  model <- DashboardDbModel(appContext)
  df <- model$queryDb("SELECT * FROM @schema.result")
  expect_true(length(df) > 1)

  count <- model$countQuery("SELECT * FROM @schema.result")
  expect_true(nrow(df) == count)
  expect_true(model$tableExists("result"))
  expect_false(model$tableExists("foo_table"))
  model$closeConnection()
  expect_false(model$connectionActive)
})

test_that("Model getter functions", {
  model <- DashboardDbModel(appContext)

  expect_data_frame(model$getExposureControls(c(1)))
  expect_data_frame(model$getOutcomeControls(c(1)))
  expect_true(length(model$getOutcomeCohortNames()) > 0)
  expect_true(length(model$getExposureCohortNames()) > 0)
  expect_true(length(model$getFilteredTableResults()) > 0)
  expect_true(length(model$getNegativeControls()) > 0)
  expect_true(length(model$getMappedAssociations()) > 0)
  expect_data_frame(model$getTimeToOutcomeStats(1, 1))
  expect_data_frame(model$getTimeOnTreatmentStats(1, 1))
  model$closeConnection()
})


test_that("load dashboard server", {
  .GlobalEnv$appContext <- appContext
  .GlobalEnv$model <- DashboardDbModel(appContext)

  shiny::testServer(dashboardInstance, {

    session$setInputs(outcomeCohorts = NULL,
                      targetCohorts = NULL,
                      exposureClass = NULL,
                      requiredDataSources = "eunomia",
                      outcomeCohortTypes = c("Inpatient"),
                      cutrange1 = 0.5,
                      cutrange2 = 2.0,
                      pCut = 0.05,
                      filterThreshold = "Not meta-analysis",
                      excludeIndications = TRUE,
                      calibrated = TRUE,
                      scBenefit = 1,
                      mainTablePageSize = 20,
                      mainTablePage = 1,
                      mainTableSortBy = "TARGET_COHORT_NAME",
                      scRisk = 1)

    expect_equal(getOutcomeCohortTypes()[[1]], 0)
    expect_int(getMainTableCount())

    data <- mainTableReac()
    expect_data_frame(data)

  })
})