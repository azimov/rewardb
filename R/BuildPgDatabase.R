#' @title
#' Build postgres schema
#' @description
#' Build the reward database schema for postgres instance from scratch
#' This will also require a CEM schema to be built which uses the OHDSI Common Evidence Model to generate the matrix
#' of known assocations for OMOP Standard Vocabulary terms. This is required for generating any stats that require negative controls
#' @param configFilePath path to global reward config
#' @param recreateCem optionally rebuild cem schema from scratch
#' @export
buildPgDatabase <- function(configFilePath = "config/global-cfg.yml", recreateCem = FALSE) {
  config <- loadGlobalConfiguration(configFilePath)
  connection <- DatabaseConnector::connect(connectionDetails = config$connectionDetails)
  on.exit(DatabaseConnector::disconnect(connection))

  message("creating rewardb results schema")
  sql <- SqlRender::readSql(system.file("sql/create", "pgSchema.sql", package = "rewardb"))
  DatabaseConnector::renderTranslateExecuteSql(
    connection,
    sql,
    schema = config$rewardbResultsSchema
  )

  sql <- SqlRender::readSql(system.file("sql/create", "referenceTables.sql", package = "rewardb"))
  DatabaseConnector::renderTranslateExecuteSql(
    connection,
    sql,
    schema = config$rewardbResultsSchema,
    include_constraints = 1
  )

  sql <- SqlRender::readSql(system.file("sql/create", "cohortReferences.sql", package = "rewardb"))
  DatabaseConnector::renderTranslateExecuteSql(
    connection,
    sql,
    vocabulary_schema = 'vocabulary',
    schema = config$rewardbResultsSchema
  )

  cemSchema <- DatabaseConnector::renderTranslateQuerySql(connection, "SELECT schema_name FROM information_schema.schemata WHERE schema_name = 'cem';")
  if (recreateCem | nrow(cemSchema) == 0) {
    sql <- SqlRender::readSql(system.file("sql/create", "cemSchema.sql", package = "rewardb"))
    DatabaseConnector::renderTranslateExecuteSql(connection, sql)
  }

  addAnalysisSettingsJson(connection, config)

}

importCemSummary <- function(summaryFilePath, configFilePath = "config/global-cfg.yml") {
  checkmate::assert_file_exists(summaryFilePath)
  config <- loadGlobalConfiguration(configFilePath)
  pgCopy(connectionDetails = config$connectionDetails, summaryFilePath, "cem", "matrix_summary")
}

addAnalysisSetting <- function(connection, config, name, typeId, description, options) {
  jsonStr <- RJSONIO::toJSON(options)
  optionsEnc <- base64enc::base64encode(charToRaw(jsonStr))
  iSql <- "INSERT INTO @schema.analysis_setting (type_id, analysis_name, description, options) VALUES('@type_id','@name','@description','@options')"
  DatabaseConnector::renderTranslateExecuteSql(connection, iSql, name = name, type_id = typeId, description = description, options = optionsEnc, schema = config$rewardbResultsSchema)
}

addAnalysisSettingsJson <- function(connection, config, settingsFilePath = system.file("settings", "default.json", package = "rewardb")) {
  for (settings in RJSONIO::fromJSON(settingsFilePath)) {
    addAnalysisSetting(connection = connection, config = config, name = settings$name, typeId = settings$typeId, description = settings$description, options = settings$options)
  }
}