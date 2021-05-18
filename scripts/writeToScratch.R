# This script is to just to stop Rhealth wrecking our work.
devtools::load_all()

writeToScratchSpace <- function(cdmConfig) {
  connection <- DatabaseConnector::connect(cdmConfig$connectionDetails)
  on.exit(DatabaseConnector::disconnect(connection))
  data <- data.frame(last_write_date_time = date())
  DatabaseConnector::insertTable(connection, cdmConfig$resultSchema, "last_write", data)
}

cdmConfigPaths <- Sys.glob("config/cdm/*.yml")

for (path in cdmConfigPaths) {
  cdmConfig <- loadCdmConfig(path)
  writeToScratchSpace(cdmConfig)
}
