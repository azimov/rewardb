buildPgDatabase <- function(configFilePath = "config/global-cfg.yml") {
  config <- yaml::read_yaml(configFilePath)
  connection <- DatabaseConnector::connect(config$rewardbDatabase)
  tryCatch({
    sql <- SqlRender::readSql(system.file("sql/create", "pgSchema.sql", package = "rewardb"))
    DatabaseConnector::renderTranslateExecuteSql(
      connection,
      sql,
      schema = config$rewardbResultsSchema
    )

    createReferenceTables(connection, config, dataSources)
  })
  DatabaseConnector::disconnect(connection)
}

importCemSummary <- function(summaryFilePath, configFilePath = "config/global-cfg.yml") {
  checkmate::assert_file_exists(summaryFilePath)

  config <- yaml::read_yaml(configFilePath)
  connection <- DatabaseConnector::connect(config$rewardbDatabase)
  tryCatch({
    # Load data frame
    evidence <- read.csv(summaryFilePath)
    # Insert in to db
    DatabaseConnector::dbAppendTable(connection, "cem.matrix_summary", evidence)
  })
  DatabaseConnector::disconnect(connection)
}