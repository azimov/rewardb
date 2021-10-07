library(rewardb)
config <- loadGlobalConfiguration(globalConfigPath)
cdmConfig <- loadCdmConfiguration(cdmConfigPath)
importReferenceTables(cdmConfig, rewardReferenceZipPath)
createCustomDrugEras(cdmConfigPath)

createRbCohorts <- function() {
  connection <- DatabaseConnector::connect(cdmConfig$connectionDetails)
  on.exit(DatabaseConnector::disconnect(connection))
  createCohorts(connection, cdmConfig)
  createOutcomeCohorts(connection, cdmConfig)
}
createRbCohorts()
generateSccResults(cdmConfigPath, config)