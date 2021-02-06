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

unlink("export")
unlink("rb-import")

test_that("Full data generation and export", {
  atlasId <- c(12047)
  refZipFile <- "atlas-pre-existing-reward-reference-test.zip"
  exportAtlasCohortRef(config, atlasId, refZipFile)

  configId <- paste("atlasRun-pre-existing")

  zipFiles <- sccOneOffAtlasCohort(cdmConfigPath, refZipFile, configId)
  for (zipFile in zipFiles) {
    importResultsZip(zipFile, unzipPath = paste(configId, "import_folder") )
  }

  atlasExposureId <- c(19321)
  exportAtlasCohortRef(config, atlasExposureId, refZipFile, exposure = TRUE)

  zipFiles <- sccOneOffAtlasCohort(cdmConfigPath, refZipFile, configId, exposure = TRUE)
  for (zipFile in zipFiles) {
    importResultsZip(zipFile, unzipPath = paste(configId, "import_folder") )
  }
})
DatabaseConnector::disconnect(connection)
unlink(refZipFile)
unlink("export")
unlink("rb-import")