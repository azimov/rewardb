configFilePath <- system.file("tests", "test.cfg.yml", package = "rewardb")
config <- loadGlobalConfig(configFilePath)
connection <- DatabaseConnector::connect(connectionDetails = config$connectionDetails)

# Set up a database with constructed cohorts etc
buildPgDatabase(configFilePath = configFilePath, buildPhenotypeLibrary = FALSE)
# Add some extra scc settings
addAnalysisSettingsJson(connection, config, settingsFilePath = system.file("tests", "testSCCSettings.json", package = "rewardb"))

cohortDefinition <- RJSONIO::fromJSON(system.file("tests", "atlasCohort12047.json", package = "rewardb"))
sqlDefinition <- readr::read_file(system.file("tests", "atlasCohort12047.sql", package = "rewardb"))
insertAtlasCohortRef(connection, config, 12047, cohortDefinition = cohortDefinition, sqlDefinition = sqlDefinition)

cohortDefinition <- RJSONIO::fromJSON(system.file("tests", "atlasExposureCohort19321.json", package = "rewardb"))
sqlDefinition <- readr::read_file(system.file("tests", "atlasExposureCohort19321.sql", package = "rewardb"))
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
importReferenceTables(cdmConfig, zipFilePath)
registerCdm(connection, config, cdmConfig)
createOutcomeCohorts(connection, cdmConfig)
createCohorts(connection, cdmConfig)

unlink("export")
unlink("rb-import")

test_that("Full data generation and export", {
  atlasId <- c(12047)
  configId <- "atlasRun-exposures"
  resultsFiles <- sccOneOffAtlasCohort(cdmConfigPath, configId, atlasIds = atlasId)

  configId <- "atlasRun-exposures"
  atlasExposureId <- c(19321)
  resultsFiles <- sccOneOffAtlasCohort(cdmConfigPath, configId, atlasIds = atlasExposureId, exposure = TRUE)

})
DatabaseConnector::disconnect(connection)
unlink("export")
unlink("rb-import")