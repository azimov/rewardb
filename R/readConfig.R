makeConnections <- function (appContext, useCdm = TRUE) {
  appContext$connection <- DatabaseConnector::connect(appContext$connectionDetails)
  if (useCdm) {
    appContext$cdmConnection <- DatabaseConnector::connect(appContext$resultsDatabase$cdmDataSource)
  }
}

cleanUp <- function (appContext) {
  DatabaseConnector::disonnect(appContext$connection)
  DatabaseConnector::disonnect(appContext$cdmConnection)
}

loadAppContext <- function (filePath, createConnection=FALSE) {
  appContext <- yaml::read_yaml(filePath)

  appContext$dbConn <- NULL
  if (createConnection) {
    makeConnections(appContext)
  }
  return(appContext)
}