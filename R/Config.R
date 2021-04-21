#' @title
#' Get password securely
#' @description
#' Gets password from user. Ignored if REWARD_B_PASSWORD system env variable is set (e.g. in .Rprofile)
#' @param envVar environment variable to store password in
#' @param promt text prompt when loading askpass prompt
#' @returns string from prompt or environment variable
getPasswordSecurely <- function(envVar = "REWARD_PASSWORD", prompt = "Enter the reward database password") {
  pass <- Sys.getenv(envVar)
  if (pass == "") {
    pass <- askpass::askpass(prompt)
    args <- list(pass)
    names(args) <- envVar
    do.call(Sys.setenv, args)
  }
  return(pass)
}

#' @title
#' Set default list option
#' @description
#' Sets the default options to the app context.
#' Will be more widely used in future iterations
#' @param config list of configuration options
#' @param defaults list of default values to check and set if null
#' @returns updated list
.setDefaultOptions <- function(config, defaults) {
  for (n in names(defaults)) {
    if (is.null(config[[n]])) {
      config[[n]] <- defaults[[n]]
    }
  }

  return(config)
}

#' @title
#' getOutcomeCohortIds
#' @description
#' Get cohorts used on dashboard
#' @param appContext application context
#' @param defaults list of default values to check and set if null
#' @returns updated list
getOutcomeCohortIds <- function(appContext, connection) {
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

#' @title
#' getTargetCohortIds
#' @description
#' Get cohorts used on dashboard
#' @param appContext application context
#' @param defaults list of default values to check and set if null
#' @returns updated list
getTargetCohortIds <- function(appContext, connection) {
  if (!length(appContext$target_concept_ids) &
    !length(appContext$custom_exposure_ids) &
    !length(appContext$atlas_exposure_ids)) {
    return(NULL)
  }

  sql <- "
    SELECT cohort_definition_id AS ID FROM @reference_schema.cohort_definition WHERE drug_conceptset_id IN (@concept_ids)
    UNION
    SELECT cohort_definition_id AS ID FROM @reference_schema.custom_exposure WHERE concept_set_id IN (@custom_exposure_ids)
    UNION
    SELECT cohort_definition_id AS ID FROM @reference_schema.atlas_exposure_reference WHERE atlas_id IN (@atlas_exposure_ids)
    "
  result <- DatabaseConnector::renderTranslateQuerySql(
    connection,
    sql,
    reference_schema = appContext$globalConfig$rewardbResultsSchema,
    concept_ids = if (length(appContext$target_concept_ids)) appContext$target_concept_ids else "NULL",
    custom_exposure_ids = if (length(appContext$custom_exposure_ids)) appContext$custom_exposure_ids else "NULL",
    atlas_exposure_ids = if (length(appContext$atlas_exposure_ids)) appContext$atlas_exposure_ids else "NULL",
  )
  return(result$ID)
}


#' @title
#' Loads Application Context
#' @description
#' By default, loads the database connections in to this object
#' loads database password from prompt if REWARD_B_PASSWORD system env variable is not set (e.g. in .Rprofile)
#' The idea is to allow shared configuration settings between the web app and any data processing tools
#' @param configPath is a yaml file for the application configuration
#' @param globalConfigPath path to global yaml
#' @param .env environment to load variable in to
#' @keywords appContext
#' @export
#' @examples
#'      appContext <- loadAppContext('config/config.dev.yml', 'config/global-cfg.yml')
loadAppContext <- function(configPath, globalConfigPath, .env = .GlobalEnv) {

  defaults <- list(
    useExposureControls = FALSE,
    custom_exposure_ids = c(),
    useConnectionPool = TRUE
  )

  appContext <- .setDefaultOptions(yaml::read_yaml(configPath), defaults)
  appContext$globalConfig <- loadGlobalConfig(globalConfigPath)
  appContext$connectionDetails <- appContext$globalConfig$connectionDetails

  class(appContext) <- append(class(appContext), "appContext")
  return(appContext)
}

#' @title
#' Loads global config
#' @description
#' @param globalConfigPath path to global yaml
#' @export
loadGlobalConfig <- function(globalConfigPath) {
  config <- yaml::read_yaml(globalConfigPath)

  if (is.null(config$connectionDetails$password)) {

    if (!is.null(config$keyringService)) {
      config$connectionDetails$password <- keyring::key_get(config$keyringService, username = config$connectionDetails$user)
    } else {
      config$connectionDetails$password <- getPasswordSecurely()
    }
  }

  config$connectionDetails <- do.call(DatabaseConnector::createConnectionDetails, config$connectionDetails)
  return(config)
}

#' @title
#' Load report application context
#' @description
#' By default, loads the database connections in to this object
#' loads database password from prompt if REWARD_B_PASSWORD system env variable is not set (e.g. in .Rprofile)
#' The idea is to allow shared configuration settings between the web app and any data processing tools
#' @param globalConfigPath is a yaml file for the application configuratione
#' @param .env environment to load variable in to
#' @param exposureId exposure cohort id
#' @param outcomeId outcome cohort id
#' @keywords reportAppContext
#' @export
#' @examples
#' loadAppContext('config/config.dev.yml', 'config/global-cfg.yml')
loadReportContext <- function(globalConfigPath) {
  reportAppContext <- loadGlobalConfig(globalConfigPath)
  reportAppContext$useConnectionPool = TRUE
  class(reportAppContext) <- append(class(reportAppContext), "reportAppContext")
  return(reportAppContext)
}

#' @title
#' load cdm config object
#' @description
#' Loads config and prompt user for db password
#' Password can be set in envrionment variable passwordEnvironmentVariable of yaml file
#' @param cdmConfigPath cdmConfigPath
#' @export
loadCdmConfig <- function(cdmConfigPath) {
  defaults <- list(
    passwordEnvironmentVariable = "UNSET_DB_PASS_VAR",
    useSecurePassword = FALSE,
    useMppBulkLoad = FALSE
  )
  config <- .setDefaultOptions(yaml::read_yaml(cdmConfigPath), defaults)

  defaultTables <- list()

  for (table in CONST_REFERENCE_TABLES) {
    defaultTables[[SqlRender::snakeCaseToCamelCase(table)]] <- table
  }

  config$tables <- .setDefaultOptions(config$tables, defaultTables)

  if (config$useSecurePassword) {

    if (!is.null(config$keyringService)) {
      config$connectionDetails$password <- keyring::key_get(config$keyringService, username = config$connectionDetails$user)
    } else {
      config$connectionDetails$password <- getPasswordSecurely(envVar = config$passwordEnvironmentVariable)
    }
  }
  config$connectionDetails <- do.call(DatabaseConnector::createConnectionDetails, config$connectionDetails)

  return(config)
}