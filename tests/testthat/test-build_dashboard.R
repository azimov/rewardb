# Set up a database with constructed cohorts etc
fullDbSetup()
runDataBuild()

test_that("Dashboard creation works", {
  Sys.setenv("REWARD_B_PASSWORD" = "postgres")
  buildDashboardFromConfig(appContextFile, configFilePath, performCalibration = TRUE)
})

appContext <- loadAppContext(appContextFile, configFilePath)
test_that("Data model utilitiy queries", {
  model <- DashboardDbModel(appContext)
  df <- model$queryDb("SELECT * FROM @schema.result")
  expect_true(length(df)  > 1)

  count <- model$countQuery("SELECT * FROM @schema.result")
  expect_true(nrow(df) == count)
  expect_true(model$tableExists("result"))
  expect_false(model$tableExists("foo_table"))
  model$closeConnection()
  expect_error(model$queryDb("SELECT * FROM @schema.result"))
})

test_that("Model getter functions", {
  model <- DashboardDbModel(appContext)

  df <- model$getExposureControls(c(1))
  df <- model$getOutcomeControls(c(1))
  expect_true(length(model$getOutcomeCohortNames()) > 0)
  expect_true(length(model$getExposureCohortNames()) > 0)
  expect_true(length(model$getFilteredTableResults()) > 0)
  expect_true(length(model$getNegativeControls()) > 0)
  expect_true(length(model$getMappedAssociations()) > 0)

  model$closeConnection()
})

