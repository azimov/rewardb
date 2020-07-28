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
        pass <- askpass::askpass("Please enter the reward b database password")
        Sys.setenv("REWARD_B_PASSWORD" = pass)
    }
    return(pass)
}

.setDefaultOptions <- function (appContext) {
    defaults <- list(
        useExposureControls = FALSE
    )

    for(n in names(defaults)) {
      if(is.null(appContext[[n]])) {
        appContext[[n]] <- defaults[[n]]
      }
    }

    return(appContext)
}

#' Loads the application configuration and creates an application object
#' By default, loads the database connections in to this object
#'
#' The idea is to allow shared configuration settings between the web app and any data processing tools
#' TODO: make S3 class with getter and setter functions for db connections handling things more cleanly
#' @param configPath is a yaml file for the application configuration
#' @param create the database connections for this app - defaults to true
#' @keywords appContext
#' @export
#' @examples
#' loadAppContext('config/config.dev.yml')
loadAppContext <- function(configPath, createConnection = FALSE, useCdm = FALSE, .env=.GlobalEnv) {
    appContext <- .setDefaultOptions(yaml::read_yaml(configPath))
    appContext$connectionDetails$password <- getPasswordSecurely()
    if (createConnection) {
      appContext$connection <- DatabaseConnector::connect(appContext$connectionDetails)
    }

    if (useCdm) {
      appContext$cdmConnection <- DatabaseConnector::connect(appContext$resultsDatabase$cdmDataSource)
    }

    class(appContext) <- append(class(appContext), "rewardb::appContext")
    .env$appContext <- appContext
}
