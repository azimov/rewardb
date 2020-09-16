buildPgDatabase <- function(configFilePath = "config/global-cfg.yml") {
  config <- yaml::read_yaml(configFilePath)

  tryCatch({
    connection <- DatabaseConnector::connect(config$rewardbDatabase)

    sql <- SqlRender::readSql(system.file("sql/create", "pgSchema.sql", package = "rewardb"))
    DatabaseConnector::renderTranslateExecuteSql(
        connection,
        sql,
        schema = config$rewardbResultsSchema
    )
  },
  finally = function () {
    DatabaseConnector::disconnect(connection)
  })
}