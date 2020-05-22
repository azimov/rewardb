loadAppContext <- function (filePath, createConnection=FALSE) {
  app <- yaml::read_yaml(filePath)
  # Create database connection
  connectionDetails <- DatabaseConnector::createConnectionDetails(cfg$connectionDetails)

  app$dbConn <- NULL
  if (createConnection) {
    app$dbConn <- DatabaseConnector::connect(app$connectionDetails)
  }

  return(app)
}