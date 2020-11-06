#' Gets password from user. Ignored if REWARD_B_PASSWORD system env variable is set (e.g. in .Rprofile)
getPasswordSecurely <- function() {
    pass <- Sys.getenv("REWARD_B_PASSWORD")
    if(pass == "") {
        pass <- askpass::askpass("Please enter the reward b database password")
        Sys.setenv("REWARD_B_PASSWORD" = pass)
    }
    return(pass)
}

#' Sets the default options to the app context.
#' Will be more widely used in future iterations
#' @param appContext list of configuration options
.setDefaultOptions <- function (appContext) {
    defaults <- list(
        useExposureControls = FALSE,
        custom_exposure_ids = c(),
        debugMode = FALSE
    )

    for(n in names(defaults)) {
      if(is.null(appContext[[n]])) {
        appContext[[n]] <- defaults[[n]]
      }
    }

    if (!is.null(appContext$target_concept_ids)) {
      appContext$targetCohortIds <- appContext$target_concept_ids * 1000
    }

    if (!is.null(appContext$outcome_concept_ids)) {
      appContext$outcomeCohortIds <- append(appContext$outcome_concept_ids * 100, appContext$outcome_concept_ids * 100 + 1)
    }

    return(appContext)
}

#' Loads the application configuration and creates an application object
#' By default, loads the database connections in to this object
#' loads database password from prompt if REWARD_B_PASSWORD system env variable is not set (e.g. in .Rprofile)
#' The idea is to allow shared configuration settings between the web app and any data processing tools
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
