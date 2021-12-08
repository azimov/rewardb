
#' Loads Application Context
#' @description
#' By default, loads the database connections in to this object
#' loads database password from prompt if REWARD_B_PASSWORD system env variable is not set (e.g. in .Rprofile)
#' The idea is to allow shared configuration settings between the web app and any data processing tools
#' @param configPath is a yaml file for the application configuration
#' @param globalConfigPath path to global yaml
#' @export
loadShinyAppContext <- function(configPath, globalConfigPath) {
  defaults <- list(
    useExposureControls = FALSE,
    custom_exposure_ids = c(),
    useConnectionPool = TRUE,
    analysisIds = c(1)
  )

  appContext <- .setDefaultOptions(yaml::read_yaml(configPath), defaults)
  appContext$globalConfig <- loadGlobalConfiguration(globalConfigPath)
  appContext$connectionDetails <- appContext$globalConfig$connectionDetails

  class(appContext) <- append(class(appContext), "appContext")
  return(appContext)
}

#' Loads global config
#' @description
#' Load reward global config yaml file
#' @param globalConfigPath path to global yaml
#' @export
loadGlobalConfiguration <- function(globalConfigPath) {
  config <- yaml::read_yaml(globalConfigPath)

  if (is.null(config$connectionDetails$user)) {
    user <- Sys.getenv("REWARD_DB_USER", "reward_user")
    config$connectionDetails$user <- user
  }

  if (is.null(config$connectionDetails$password)) {

    if (!is.null(config$keyringService)) {
      config$connectionDetails$password <- keyring::key_get(config$keyringService, username = config$connectionDetails$user)
    } else {
      stop("Set password securely with keyringService option and using keyring::key_set with the database username.")
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
loadReportContext <- function(globalConfigPath) {
  reportAppContext <- loadGlobalConfiguration(globalConfigPath)
  reportAppContext$useConnectionPool <- TRUE
  class(reportAppContext) <- append(class(reportAppContext), "reportAppContext")
  return(reportAppContext)
}

#' load cdm config object
#' @description
#' Loads config and prompt user for db password
#' Password can be set in envrionment variable passwordEnvironmentVariable of yaml file
#' @param cdmConfigPath cdmConfigPath
#' @export
loadCdmConfiguration <- function(cdmConfigPath) {
  defaults <- list(
    passwordEnvironmentVariable = "UNSET_DB_PASS_VAR",
    useSecurePassword = FALSE,
    bulkUpload = FALSE,
    performActiveDataTransfer = FALSE
  )
  config <- .setDefaultOptions(yaml::read_yaml(cdmConfigPath), defaults)

  defaultTables <- list()

  for (table in CONST_REFERENCE_TABLES) {
    defaultTables[[SqlRender::snakeCaseToCamelCase(table)]] <- table
  }

  config$tables <- .setDefaultOptions(config$tables, defaultTables)

  if (is.null(config$connectionDetails$user)) {
    user <- Sys.getenv("REWARD_CDM_USER", Sys.info()[["user"]])
    config$connectionDetails$user <- user
  }

  if (config$useSecurePassword) {

    if (!is.null(config$keyringService)) {
      config$connectionDetails$password <- keyring::key_get(config$keyringService, username = config$connectionDetails$user)
    } else {
      stop("Set password securely with keyringService option and using keyring::key_set with the database username.")
    }
  }
  config$connectionDetails <- do.call(DatabaseConnector::createConnectionDetails, config$connectionDetails)

  return(config)
}



.setDefaultOptions <- function(config, defaults) {
  for (n in names(defaults)) {
    if (is.null(config[[n]])) {
      config[[n]] <- defaults[[n]]
    }
  }

  return(config)
}


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

