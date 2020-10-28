#' Gets password from user. Ignored if REWARD_B_PASSWORD system env variable is set (e.g. in .Rprofile)
getPasswordSecurely <- function(.envVar = "REWARD_B_PASSWORD") {
    pass <- Sys.getenv(".envVar")
    if(pass == "") {
        pass <- askpass::askpass("Please enter the reward b database password")
        Sys.setenv(.envVar = pass)
    }
    return(pass)
}

#' Sets the default options to the app context.
#' Will be more widely used in future iterations
#' @param appContext list of configuration options
.setDefaultOptions <- function (appContext) {
    defaults <- list(
        useExposureControls = FALSE,
        custom_exposure_ids = c()
    )

    for(n in names(defaults)) {
      if(is.null(appContext[[n]])) {
        appContext[[n]] <- defaults[[n]]
      }
    }

    return(appContext)
}

getOutcomeCohortIds <- function (appContext, connection) {
    if (!length(appContext$outcome_concept_ids) & !length(appContext$custom_outcome_cohort_ids)) {
        return(NULL)
    }

    sql <- "
    SELECT cohort_definition_id AS ID FROM @reference_schema.outcome_cohort_definition WHERE conceptset_id IN (@concept_ids)
    UNION
    SELECT cohort_definition_id AS ID FROM @reference_schema.atlas_outcome_reference WHERE atlas_id IN (@atlas_ids)
    "
    result <- DatabaseConnector::renderTranslateQuerySql(
      connection,
      sql,
      reference_schema = appContext$globalConfig$rewardbResultsSchema,
      concept_ids = if (length(appContext$outcome_concept_ids)) appContext$outcome_concept_ids else "NULL",
      atlas_ids = if (length(appContext$custom_outcome_cohort_ids)) appContext$custom_outcome_cohort_ids else "NULL",
    )
    return(result$ID)
}

getTargetCohortIds <- function (appContext, connection) {
    if (!length(appContext$target_concept_ids) & !length(appContext$custom_exposure_ids)) {
        return(NULL)
    }

    sql <- "
    SELECT cohort_definition_id AS ID FROM @reference_schema.cohort_definition WHERE drug_conceptset_id IN (@concept_ids)
    UNION
    SELECT cohort_definition_id AS ID FROM @reference_schema.custom_exposure WHERE concept_set_id IN (@custom_exposure_ids)
    "
    result <- DatabaseConnector::renderTranslateQuerySql(
      connection,
      sql,
      reference_schema = appContext$globalConfig$rewardbResultsSchema,
      concept_ids = if (length(appContext$outcome_concept_ids)) appContext$outcome_concept_ids else "NULL",
      custom_exposure_ids = if (length(appContext$custom_exposure_ids)) appContext$custom_exposure_ids else "NULL",
    )
    return(result$ID)
}


#' Loads the application configuration and creates an application object
#' @description
#' By default, loads the database connections in to this object
#' loads database password from prompt if REWARD_B_PASSWORD system env variable is not set (e.g. in .Rprofile)
#' The idea is to allow shared configuration settings between the web app and any data processing tools
#' @param configPath is a yaml file for the application configuration
#' @param create the database connections for this app - defaults to true
#' @keywords appContext
#' @export
#' @examples
#' loadAppContext('config/config.dev.yml', 'config/global-cfg.yml')
loadAppContext <- function(configPath, globalConfigPath, .env=.GlobalEnv) {
    appContext <- .setDefaultOptions(yaml::read_yaml(configPath))
    appContext$globalConfig <- loadGlobalConfig(globalConfigPath)
    appContext$connectionDetails <- appContext$globalConfig$connectionDetails

    class(appContext) <- append(class(appContext), "rewardb::appContext")
    .env$appContext <- appContext
}

# Path for config shared across shiny apps and data build
loadGlobalConfig <- function(globalConfigPath) {
    config <- yaml::read_yaml(globalConfigPath)

    if (is.null(config$connectionDetails$password)) {
        config$connectionDetails$password <- getPasswordSecurely()
    }
    return(config)
}