configFilePath <- system.file("tests", "test.cfg.yml", package = "rewardb")
config <- loadGlobalConfig(configFilePath)
connection <- DatabaseConnector::connect(connectionDetails = config$connectionDetails)

# Set up a database with constructed cohorts etc
buildPgDatabase(configFilePath = configFilePath, buildPhenotypeLibrary = FALSE)
cohortDefinition <- RJSONIO::fromJSON(system.file("tests", "atlasCohort1.json", package = "rewardb"))
sqlDefinition <- readr::read_file(system.file("tests", "atlasCohort1.sql", package = "rewardb"))
insertAtlasCohortRef(connection, config, 1, cohortDefinition = cohortDefinition, sqlDefinition = sqlDefinition)

cohortDefinition <- RJSONIO::fromJSON(system.file("tests", "atlasCohort12047.json", package = "rewardb"))
sqlDefinition <- readr::read_file(system.file("tests", "atlasCohort12047.sql", package = "rewardb"))
insertAtlasCohortRef(connection, config, 12047, cohortDefinition = cohortDefinition, sqlDefinition = sqlDefinition)

cohortDefinition <- RJSONIO::fromJSON(system.file("tests", "atlasExposureCohort19321.json", package = "rewardb"))
sqlDefinition <- readr::read_file(system.file("tests", "atlasCohort19321.sql", package = "rewardb"))
insertAtlasCohortRef(connection, config, 19321, cohortDefinition = cohortDefinition, sqlDefinition = sqlDefinition, exposure = TRUE)

conceptSetId <- 11933
conceptSetDefinition <- RJSONIO::fromJSON(system.file("tests", "conceptSet1.json", package = "rewardb"))
insertCustomExposureRef(connection, config, conceptSetId, "Test Exposure Cohort", conceptSetDefinition = conceptSetDefinition)

cdmConfigPath <- system.file("tests", "eunomia.cdm.cfg.yml", package = "rewardb")
cdmConfig <- loadCdmConfig(cdmConfigPath)

zipFilePath <- "rewardb-references.zip"
refFolder <- "reference_test_folder"
unlink(zipFilePath)
unlink(refFolder)
exportReferenceTables(config)
print("import refs")
importReferenceTables(cdmConfig, zipFilePath)
print("scc results")
generateSccResults(cdmConfigPath, .getDbId = FALSE)
importResultsFiles(config$connectionDetails, "test", "reward-b-scc-results-aid-1.zip", .debug=TRUE)

appContextFile <- system.file("tests", "test.dashboard.yml", package = "rewardb")

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
  expect_true(length(df) == count)
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

DatabaseConnector::disconnect(connection)