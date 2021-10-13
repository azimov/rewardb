.requiredVars <- c("globalConfigPath", "cdmConfigPath", "rewardReferenceZipPath")
.setVars <- sapply(.requiredVars, exists)
if (!any(.setVars)) {
  errorMesage <- paste("\nRequired variable:", names(.setVars)[!.unsetVars], "is not set.")
  stop(errorMesage)
}

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