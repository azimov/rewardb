buildPgDatabase <- function(configFilePath = "config/global-cfg.yml") {
  config <- loadGlobalConfig(configFilePath)
  connection <- DatabaseConnector::connect(connectionDetails = config$connectionDetails)
  tryCatch({
    message("creating rewardb results schema")
    sql <- SqlRender::readSql(system.file("sql/create", "pgSchema.sql", package = "rewardb"))
    DatabaseConnector::renderTranslateExecuteSql(
      connection,
      sql,
      vocabulary_schema = "vocabulary",
      schema = config$rewardbResultsSchema
    )
  })
  DatabaseConnector::disconnect(connection)
}

importCemSummary <- function(summaryFilePath, configFilePath = "config/global-cfg.yml") {
  checkmate::assert_file_exists(summaryFilePath)

  config <- loadGlobalConfig(configFilePath)
  connection <- DatabaseConnector::connect(connectionDetails = config$connectionDetails)
  tryCatch({
    # Load data frame
    evidence <- read.csv(summaryFilePath)
    # Insert in to db
    DatabaseConnector::dbAppendTable(connection, "cem.matrix_summary", evidence)
  })
  DatabaseConnector::disconnect(connection)
}