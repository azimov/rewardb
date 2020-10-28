configFilePath <- system.file("tests", "test.cfg.yml", package = "rewardb")
config <- loadGlobalConfig(configFilePath)
connection <- DatabaseConnector::connect(config$connectionDetails)

# Set up a database with constructed cohorts etc
buildPgDatabase(configFilePath = configFilePath)
cohortDefinition <- RJSONIO::fromJSON(system.file("tests", "atlasCohort1.json", package = "rewardb"))
sqlDefinition <- readr::read_file(system.file("tests", "atlasCohort1.sql", package = "rewardb"))
insertAtlasCohortRef(connection, config, 1, cohortDefinition = cohortDefinition, sqlDefinition = sqlDefinition)

cohortDefinition <- RJSONIO::fromJSON(system.file("tests", "atlasCohort12047.json", package = "rewardb"))
sqlDefinition <- readr::read_file(system.file("tests", "atlasCohort12047.sql", package = "rewardb"))
insertAtlasCohortRef(connection, config, 12047, cohortDefinition = cohortDefinition, sqlDefinition = sqlDefinition)

conceptSetId <- 11933
conceptSetDefinition <- RJSONIO::fromJSON(system.file("tests", "conceptSet1.json", package = "rewardb"))
insertCustomExposureRef(connection, config, conceptSetId, "Test Exposure Cohort", conceptSetDefinition = conceptSetDefinition)

cdmConfigPath <- system.file("tests", "eunomia.cdm.cfg.yml", package = "rewardb")
cdmConfig <- yaml::read_yaml(cdmConfigPath)

zipFilePath <- "rewardb-references.zip"
refFolder <- "reference_test_folder"
unlink(zipFilePath)
unlink(refFolder)
exportReferenceTables(config)
importReferenceTables(cdmConfig, zipFilePath, refFolder)
generateSccResults(cdmConfigPath)
importResultsFiles(config$connectionDetails, "test", "rewardb-export.zip")

test_that("Dashboard creation works", {
  Sys.setenv("REWARD_B_PASSWORD" = "postgres")
  appContext <- loadAppContext(system.file("tests", "test.dashboard.yml", package = "rewardb"), configFilePath)
  expect_is(appContext, "rewardb::appContext")
  createDashSchema(appContext = appContext, connection = connection)

  addCemEvidence(appContext, connection)

  computeMetaAnalysis(appContext, connection)

  .removeCalibratedResults(appContext, connection)
  if (appContext$useExposureControls) {
    calibrateOutcomes(appContext, connection)
  } else {
    calibrateTargets(appContext, connection)
  }

})