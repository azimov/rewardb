set.seed(1337)
configFilePath <- file.path("testCfg", "test.cfg.yml")
config <- loadGlobalConfiguration(configFilePath)
connection <- DatabaseConnector::connect(connectionDetails = config$connectionDetails)

cdmConfigPath <- file.path("testCfg", "eunomia.cdm.cfg.yml")
cdmConfig <- loadCdmConfiguration(cdmConfigPath)

zipFilePath <- "rewardb-references.zip"
refFolder <- "reference_test_folder"
unlink(zipFilePath)
unlink(refFolder)
cemConnection <- getCemConnection()

# Cleanup test dir
withr::defer({
  cemConnection$finalize()
  # Remove any fake drug eras
  delStment <- "DELETE FROM eunomia.drug_era WHERE drug_era_id IS NULL"
  DatabaseConnector::renderTranslateExecuteSql(connection, delStment)
  unlink(zipFilePath, recursive = TRUE, force = TRUE)
  unlink(refFolder, recursive = TRUE, force = TRUE)
  unlink(cdmConfig$referenceFolder, recursive = TRUE, force = TRUE)
  unlink("reference_files", recursive = TRUE, force = TRUE)
  unlink("export", recursive = TRUE, force = TRUE)
  unlink("rb-import", recursive = TRUE, force = TRUE)
  DatabaseConnector::disconnect(connection)
}, testthat::teardown_env())

# Strep is common in eunomia, negative controls for it are not
strepConceptId <- 28060
controls <- cemConnection$getSuggestedControlIngredients(conditionConceptSet = data.frame(conceptId=c(strepConceptId)))
# Insert simulated negative control drug eras for Strep concept
for (controlConceptId in controls$conceptId) {
  insertSimulatedExposure(connection,
                          strepConceptId,
                          controlConceptId,
                          sample(1:50, 1),
                          sample(1:50, 1))
}

controls$domainId <- "Drug"
controls$vocabularyId <- "RxNorm"
controls$conceptClassId <- "Ingredient"
controls$standardConcept <- "S"
controls$conceptCode <- controls$conceptId
controls$validStartDate <- as.Date("1970-01-01")
controls$validEndDate <- as.Date("2099-12-31")

DatabaseConnector::insertTable(connection,
                               data = controls,
                               databaseSchema = "eunomia",
                               tableName = "concept",
                               dropTableIfExists = FALSE,
                               camelCaseToSnakeCase = TRUE,
                               createTable = FALSE)

DatabaseConnector::insertTable(connection,
                               data = controls,
                               databaseSchema = "vocabulary",
                               tableName = "concept",
                               dropTableIfExists = FALSE,
                               camelCaseToSnakeCase = TRUE,
                               createTable = FALSE)


appContextFile <- file.path("testCfg", "test.dashboard.yml")

pgDbSetup <- function() {
  buildPgDatabase(configFilePath = configFilePath, recreateCem = TRUE)
}

setupAtlasCohorts <- function() {
  cohortDefinition <- RJSONIO::fromJSON(file.path("testCfg", "atlasCohort1.json"))
  sqlDefinition <- readr::read_file(file.path("testCfg", "atlasCohort1.sql"))
  insertAtlasCohortRef(connection, config, 1, cohortDefinition = cohortDefinition, sqlDefinition = sqlDefinition)

  cohortDefinition <- RJSONIO::fromJSON(file.path("testCfg", "atlasCohort12047.json"))
  sqlDefinition <- readr::read_file(file.path("testCfg", "atlasCohort12047.sql"))
  insertAtlasCohortRef(connection, config, 12047, cohortDefinition = cohortDefinition, sqlDefinition = sqlDefinition)

  cohortDefinition <- RJSONIO::fromJSON(file.path("testCfg", "atlasExposureCohort19321.json"))
  sqlDefinition <- readr::read_file(file.path("testCfg", "atlasExposureCohort19321.sql"))
  insertAtlasCohortRef(connection, config, 19321, cohortDefinition = cohortDefinition, sqlDefinition = sqlDefinition, exposure = TRUE)

  conceptSetId <- 11933
  conceptSetDefinition <- RJSONIO::fromJSON(file.path("testCfg", "conceptSet1.json"))
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
  createCustomDrugEras(cdmConfigPath)
  createCohorts(connection, cdmConfig)
  createOutcomeCohorts(connection, cdmConfig)
  generateSccResults(cdmConfigPath, config)
  for (file in Sys.glob(paste0(cdmConfig$exportPath, "/*.csv"))) {
    pgCopy(config$connectionDetails, file, config$rewardbResultsSchema, "scc_result")
  }
}

