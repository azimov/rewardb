#' Shiny App Database Model Calls
#' @description
#' Approach to allow data analysis inside and outside of shiny apps
#'
#'
DbModel <- setRefClass("DbModel", fields = c("appContext", "dbConn"))
DbModel$methods(
  initialize = function(appContext) {
    appContext <<- appContext
    initializeConnection()
  },

  initializeConnection = function () {
    dbConn <<- DatabaseConnector::connect(connectionDetails = appContext$connectionDetails)
  },

  exit = function() {
    DatabaseConnector::disconnect(dbConn)
  },

  queryDb = function(query, ...) {
    tryCatch({
      df <- DatabaseConnector::renderTranslateQuerySql(dbConn, query, schema = appContext$short_name, ...)
      return(df)
    },
    error = function(e) {
      ParallelLogger::logError(e)
      DatabaseConnector::disconnect(dbConn)
      dbConn <<- DatabaseConnector::connect(connectionDetails = appContext$connectionDetails)
    })
  },
  # Will only work with postgres > 9.4
  tableExists = function(tableName) {
    return(!is.na(queryDb("SELECT to_regclass('@schema.@table');", table = tableName))[[1]])
  },

  getExposureControls = function(outcomeIds) {
    return(rewardb::getExposureControls(appContext, dbConn, outcomeIds))
  },

  getOutcomeControls = function(targetIds) {
    return(rewardb::getOutcomeControls(appContext, dbConn, targetIds))
  }
)