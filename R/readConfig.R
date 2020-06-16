#' @export
cleanUpAppContext <- function(appContext) {
    DatabaseConnector::disconnect(appContext$connection)
    appContext$connection <- NULL
    DatabaseConnector::disconnect(appContext$cdmConnection)
    appContext$cdmConnection <- NULL
    return(appContext)
}

getPasswordSecurely <- function() {
    pass <- Sys.getenv("REWARD_B_PASSWORD")
    if(pass == "") {
        return(askpass::askpass("Please enter the reward b database password"))
    }
    return(pass)
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
loadAppContext <- function(filePath, createConnection = FALSE, useCdm = FALSE) {
    appContext <- yaml::read_yaml(filePath)
    appContext$connectionDetails$password <- getPasswordSecurely()
    if (createConnection) {
      appContext$connection <- DatabaseConnector::connect(appContext$connectionDetails)
    }

    if (useCdm) {
      appContext$cdmConnection <- DatabaseConnector::connect(appContext$resultsDatabase$cdmDataSource)
    }
    return(appContext)
}
