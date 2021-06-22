configFilePath <- system.file("tests", "test.cfg.yml", package = "rewardb")
config <- loadGlobalConfig(configFilePath)
connection <- DatabaseConnector::connect(connectionDetails = config$connectionDetails)

cdmConfigPath <- system.file("tests", "eunomia.cdm.cfg.yml", package = "rewardb")
cdmConfig <- loadCdmConfig(cdmConfigPath)

zipFilePath <- "rewardb-references.zip"
refFolder <- "reference_test_folder"
unlink(zipFilePath)
unlink(refFolder)

# Cleanup test dir
withr::defer({
  unlink(zipFilePath, recursive = TRUE, force = TRUE)
  unlink(refFolder, recursive = TRUE, force = TRUE)
  unlink(cdmConfig$referenceFolder, recursive = TRUE, force = TRUE)
  unlink("reference_files", recursive = TRUE, force = TRUE)
  unlink("export", recursive = TRUE, force = TRUE)
  unlink("rb-import", recursive = TRUE, force = TRUE)
  DatabaseConnector::disconnect(connection)
}, testthat::teardown_env())

appContextFile <- system.file("tests", "test.dashboard.yml", package = "rewardb")

pgDbSetup <- function () {
  buildPgDatabase(configFilePath = configFilePath, buildPhenotypeLibrary = FALSE, recreateCem = TRUE)
  importCemSummary(system.file("tests", "matrix_summary.csv", package = "rewardb"), configFilePath = configFilePath)
}

setupAtlasCohorts <- function() {
  cohortDefinition <- RJSONIO::fromJSON(system.file("tests", "atlasCohort1.json", package = "rewardb"))
  sqlDefinition <- readr::read_file(system.file("tests", "atlasCohort1.sql", package = "rewardb"))
  insertAtlasCohortRef(connection, config, 1, cohortDefinition = cohortDefinition, sqlDefinition = sqlDefinition)

  cohortDefinition <- RJSONIO::fromJSON(system.file("tests", "atlasCohort12047.json", package = "rewardb"))
  sqlDefinition <- readr::read_file(system.file("tests", "atlasCohort12047.sql", package = "rewardb"))
  insertAtlasCohortRef(connection, config, 12047, cohortDefinition = cohortDefinition, sqlDefinition = sqlDefinition)

  cohortDefinition <- RJSONIO::fromJSON(system.file("tests", "atlasExposureCohort19321.json", package = "rewardb"))
  sqlDefinition <- readr::read_file(system.file("tests", "atlasExposureCohort19321.sql", package = "rewardb"))
  insertAtlasCohortRef(connection, config, 19321, cohortDefinition = cohortDefinition, sqlDefinition = sqlDefinition, exposure = TRUE)

  conceptSetId <- 11933
  conceptSetDefinition <- RJSONIO::fromJSON(system.file("tests", "conceptSet1.json", package = "rewardb"))
  insertCustomExposureRef(connection, config, conceptSetId, "Test Exposure Cohort", conceptSetDefinition = conceptSetDefinition)

}

fullDbSetup <- function() {
  pgDbSetup()
  setupAtlasCohorts()
  registerCdm(connection, config, cdmConfig)
  exportReferenceTables(config)
  importReferenceTables(cdmConfig, zipFilePath)
}

runDataBuild <- function() {
  resultsFiles <- generateSccResults(cdmConfigPath)
  for (table in names(resultsFiles)) {
    for (file in resultsFiles[[table]]) {
      pgCopy(config$connectionDetails, file, config$rewardbResultsSchema, table, fileEncoding = "UTF-8-BOM")
    }
  }
}

