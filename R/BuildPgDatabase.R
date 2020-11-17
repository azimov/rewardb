buildPgDatabase <- function(configFilePath = "config/global-cfg.yml", buildCem = TRUE) {
  config <- loadGlobalConfig(configFilePath)
  connection <- DatabaseConnector::connect(connectionDetails = config$connectionDetails)
  tryCatch({
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
      schema = config$rewardbResultsSchema
    )

    sql <- SqlRender::readSql(system.file("sql/create", "cohortReferences.sql", package = "rewardb"))
    DatabaseConnector::renderTranslateExecuteSql(
      connection,
      sql,
      vocabulary_schema = 'vocabulary',
      schema = config$rewardbResultsSchema
    )

    sql <- SqlRender::readSql(system.file("sql/create", "cemSchema.sql", package = "rewardb"))
    DatabaseConnector::renderTranslateExecuteSql(connection, sql)
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