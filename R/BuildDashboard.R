#' @title
#' Create Dashboard Schema
#' @description
#' Creates postgres db dashboard schema for study
#' @param appContext application context loaded from yaml
#' @param connection DatabaseConnector connection
createDashSchema <- function(appContext, connection) {
  DatabaseConnector::executeSql(connection, paste("DROP SCHEMA IF EXISTS", appContext$short_name, "CASCADE;"))
  DatabaseConnector::executeSql(connection, paste("CREATE SCHEMA ", appContext$short_name))

  ParallelLogger::logInfo("Creating tables")
  pathToSqlFile <- system.file("sql/create", "rewardSchema.sql", package = "rewardb")
  sql <- SqlRender::readSql(pathToSqlFile)
  sql <- SqlRender::render(sql, schema = appContext$short_name)
  DatabaseConnector::executeSql(connection, sql = sql)

  ParallelLogger::logInfo("Mapping concepts to cohorts")
  outcomeCohortIds <- getOutcomeCohortIds(appContext, connection)
  targetCohortIds <- getTargetCohortIds(appContext, connection)

  ParallelLogger::logInfo("Importing references")
  pathToSqlFile <- system.file("sql/export", "buildDashboard.sql", package = "rewardb")
  sql <- SqlRender::readSql(pathToSqlFile)
  sql <- DatabaseConnector::renderTranslateExecuteSql(
    connection,
    sql,
    schema = appContext$short_name,
    outcome_cohort_ids_length = length(outcomeCohortIds) > 0,
    target_cohort_ids_length = length(targetCohortIds) > 0,
    target_cohort_ids = targetCohortIds,
    outcome_cohort_ids = outcomeCohortIds,
    vocabulary_schema = appContext$globalConfig$vocabularySchema,
    results_database_schema = appContext$globalConfig$rewardbResultsSchema,
    source_ids = appContext$dataSources
  )
}

#' @title
#' Add CEM based associations
#' @description
#' This is used for the automated construction of negative control sets and the indication labels
#' @param appContext application context loaded from yaml
#' @param connection DatabaseConnector connection to cdm
addCemEvidence <- function(appContext, connection) {
  connectionDetails <- DatabaseConnector::createConnectionDetails(dbms = Sys.getenv("CEM_DATABASE_DBMS"),
                                                                  server = Sys.getenv("CEM_DATABASE_SERVER"),
                                                                  user = Sys.getenv("CEM_DATABASE_USER"),
                                                                  port = Sys.getenv("CEM_DATABASE_PORT"),
                                                                  extraSettings = Sys.getenv("CEM_DATABASE_EXTRA_SETTINGS"),
                                                                  password = keyring::key_get(Sys.getenv("CEM_KEYRING_SERVICE"),
                                                                                              user = Sys.getenv("CEM_DATABASE_USER")))
  cemBackend <- CemConnector::CemDatabaseBackend$new(connectionDetails,
                                                     cemSchema = Sys.getenv("CEM_DATABASE_SCHEMA"),
                                                     vocabularySchema = Sys.getenv("CEM_DATABASE_VOCAB_SCHEMA"),
                                                     sourceSchema = Sys.getenv("CEM_DATABASE_INFO_SCHEMA"))

  # Get exposure concepts for which there are actually results for
  model <- DashboardDbModel(appContext)
  sql <- "
  SELECT DISTINCT tc.*
  from @schema.target_concept tc
  INNER JOIN @schema.result r ON r.target_cohort_id = tc.target_cohort_id
  WHERE rr IS NOT NULL"
  exposureCohortConceptSets <- model$queryDb(sql, snakeCaseToCamelCase = TRUE)
  # Get mapped negative control evidence, keeps targetCohortId
  negativControlConcepts <- exposureCohortConceptSets %>%
    dplyr::group_by(targetCohortId) %>%
    dplyr::group_modify(~cemBackend$getSuggestedControlCondtions(.x, nControls = 1000), .keep = TRUE) %>%
    select(targetCohortId, conceptId)

  # Map to outcome cohort identifiers
  mapConceptToCohorts <- function(conceptIds) {
    sql <- "SELECT oc.outcome_cohort_id
    FROM @schema.outcome_concept oc
    INNER JOIN @schema.outcome o ON o.outcome_cohort_id = oc.outcome_cohort_id
    INNER JOIN vocabulary.concept_ancestor ca ON ca.descendant_concept_id = oc.outcome_cohort_id
    WHERE ca.ancestor_concept_id IN (@concept_ids)
    AND o.type_id = @type_id
    "
    model$queryDb(sql, concept_ids = conceptIds, type_id = 2, snakeCaseToCamelCase = TRUE)
  }

  negativControlCohorts <- negativControlConcepts %>%
    dplyr::group_by(targetCohortId) %>%
      dplyr::group_modify(~mapConceptToCohorts(.x$conceptId), .keep = TRUE)

  message(paste("Mapped", nrow(negativControlCohorts), "Controls"))
  # Add to negative controls table
  DatabaseConnector::insertTable(connection,
                                 data = negativControlCohorts,
                                 databaseSchema = appContext$short_name,
                                 tableName = "negative_control",
                                 dropTableIfExists = TRUE,
                                 camelCaseToSnakeCase = TRUE,
                                 createTable = TRUE)
}

#' @title
#' meta analysis
#' @description
#' compute meta-analysis across data sources provided in table
#' Perform meta-analysis on data sources
#' @param table data.frame
#' @return data.frame - results of meta analysis
metaAnalysis <- function(table) {
  # Compute meta analysis with random effects model
  results <- meta::metainc(
    data = table,
    event.e = T_CASES,
    time.e = T_PT,
    event.c = C_CASES,
    time.c = C_PT,
    sm = "IRR",
    model.glmm = "UM.RS"
  )

  row <- data.frame(
    SOURCE_ID = -99,
    T_AT_RISK = sum(table$T_AT_RISK),
    T_PT = sum(table$T_PT),
    T_CASES = sum(table$T_CASES),
    C_AT_RISK = sum(table$C_AT_RISK),
    C_PT = sum(table$C_PT),
    C_CASES = sum(table$C_CASES),
    RR = exp(results$TE.random),
    SE_LOG_RR = results$seTE.random,
    LB_95 = exp(results$lower.random),
    UB_95 = exp(results$upper.random),
    P_VALUE = results$pval.random,
    I2 = results$I2
  )

  return(row)
}

#' @title
#' compute meta analysis
#' @description
#' Runs and saves metanalayis on data, uploads back to db
#' @param appContext application context loaded from yaml
#' @param connection DatabaseConnector connection to cdm
computeMetaAnalysis <- function(appContext, connection) {
  library(dplyr, warn.conflicts = FALSE)
  fullResults <- DatabaseConnector::renderTranslateQuerySql(
    connection,
    "SELECT * FROM @schema.result WHERE source_id != -99;",
    schema = appContext$short_name
  )

  # For each distinct pair: (target, outcome) get all data sources
  # Run meta analysis
  # Write uncalibrated table
  # Calibrate meta analysis results
  results <- fullResults %>%
    group_by(TARGET_COHORT_ID, OUTCOME_COHORT_ID) %>%
    group_modify(~metaAnalysis(.x))

  results$STUDY_DESIGN <- "scca"
  results$CALIBRATED <- 0
  pgCopyDataFrame(connectionDetails = appContext$connectionDetails, results, appContext$short_name, "result")
}

#' @title
#' Build dashboard from config file
#' @description
#' Builds a rewardb dashboard for all exposures for subset outcomes or all outcomes for subset exposures
#' Requires the rewardb results to be generated already.
#' This exports the data to a db backend and allows the config file to be used to run the shiny dashboard
#' @param filePath - path to a yaml configuration file used
#' @param globalConfigPath path to global reward config containing postgres db connection details
#' @param performCalibration - use empirical calibration package to compute adjusted p values, effect estimates and confidence intervals
#' @param allowUserAccess - enables grant permission for read only database user
#' @export
buildDashboardFromConfig <- function(filePath, globalConfigPath, performCalibration = TRUE, allowUserAccess = TRUE) {
  appContext <- loadShinyAppContext(filePath, globalConfigPath)
  connection <- DatabaseConnector::connect(connectionDetails = appContext$connectionDetails)
  on.exit(DatabaseConnector::disconnect(connection))

  logger <- .getLogger(paste0(appContext$short_name, "DashboardCreation.log"))
  tryCatch(
  {
    message("Creating schema")
    createDashSchema(appContext, connection)
    message("Running meta analysis")
    computeMetaAnalysis(appContext, connection)
    message("Adding negative controls from CEM")
    addCemEvidence(appContext, connection)

    if (performCalibration) {
      performCalibration(appContext, connection)
    }

    if (allowUserAccess) {
      grantReadOnlyUserPermissions(connection, appContext$short_name)
    }
  },
    error = ParallelLogger::logError
  )
}

performCalibration <- function(appContext, connection = NULL) {
  if (is.null(connection)) {
    connection <- DatabaseConnector::connect(appContext$connectionDetails)
    on.exit(DatabaseConnector::disconnect(connection))
  }

  .removeCalibratedResults(appContext, connection)
  if (appContext$useExposureControls) {
    message("Calibrating outcomes")
    calibratedData <- getCalibratedOutcomes(appContext, connection)
  } else {
    message("Calibrating targets")
    calibratedData <- getCalibratedTargets(appContext, connection)
  }
  pgCopyDataFrame(appContext$connectionDetails, calibratedData, appContext$short_name, "result")

}

grantReadOnlyUserPermissions <- function(connection, schema) {
  pathToSqlFile <- system.file("sql/postgresql", "grantPermissions.sql", package = "rewardb")
  sql <- SqlRender::readSql(pathToSqlFile)
  DatabaseConnector::renderTranslateExecuteSql(connection, sql, schema = schema)
}