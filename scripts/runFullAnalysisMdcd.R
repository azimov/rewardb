library(rewardb)
config <- loadGlobalConfiguration("config/global-cfg.yml")

cdmConfigPath <- "config/cdm/mdcd.yml"

cdmConfig <- loadCdmConfiguration(cdmConfigPath)
importReferenceTables(cdmConfig, "rewardb-references.zip")
createCustomDrugEras(cdmConfigPath)

connection <- DatabaseConnector::connect(cdmConfig$connectionDetails)
createCohorts(connection, cdmConfig)
createOutcomeCohorts(connection, cdmConfig)
DatabaseConnector::disconnect(connection)

resultsFiles <- generateSccResults(cdmConfigPath, .createExposureCohorts = FALSE, .createOutcomeCohorts = FALSE)
# Copy files
for (table in names(resultsFiles)) {
  for (file in resultsFiles[[table]]) {
    pgCopy(config$connectionDetails, file, config$rewardbResultsSchema, table, fileEncoding = "UTF-8-BOM")
  }
}