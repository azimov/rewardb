makeConnections <- function(appContext, useCdm = TRUE) {
    appContext$connection <- DatabaseConnector::connect(appContext$connectionDetails)
    if (useCdm) {
        appContext$cdmConnection <- DatabaseConnector::connect(appContext$resultsDatabase$cdmDataSource)
    }
}

cleanUpAppContext <- function(appContext) {
    DatabaseConnector::disonnect(appContext$connection)
    DatabaseConnector::disonnect(appContext$cdmConnection)
}

#' Loads the application configuration and creates an application object
#' By default, loads the database connections in to this object
#'
#' The idea is to allow shared configuration settings between the web app and any data processing tools
#' TODO: make S3 class with getter and setter functions for db connections handling things more cleanly
#' @param filePath is a yaml file for the application configuration
#' @param create the database connections for this app - defaults to true
#' @keywords appContext
#' @export
#' @examples
#' loadAppContext('config/config.dev.yml')
loadAppContext <- function(filePath, createConnection = FALSE) {
    appContext <- yaml::read_yaml(filePath)
    
    appContext$dbConn <- NULL
    if (createConnection) {
        makeConnections(appContext)
    }
    return(appContext)
}
