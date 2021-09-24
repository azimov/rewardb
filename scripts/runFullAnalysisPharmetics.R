library(rewardb)
config <- loadGlobalConfiguration("config/global-cfg.yml")

cdmConfigPath <- "config/cdm/pharmetrics.yml"

cdmConfig <- loadCdmConfiguration(cdmConfigPath)
importReferenceTables(cdmConfig, "rewardb-references.zip")
createCustomDrugEras(cdmConfigPath)

connection <- DatabaseConnector::connect(cdmConfig$connectionDetails)
createCohorts(connection, cdmConfig)
createOutcomeCohorts(connection, cdmConfig)
DatabaseConnector::disconnect(connection)

generateSccResults(cdmConfigPath, config)

for (file in Sys.glob(paste0(cdmConfig$exportPath, "/*.csv"))) {
  pgCopy(config$connectionDetails, file, config$rewardbResultsSchema, "scc_result")
}
