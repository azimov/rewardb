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

  initializeConnection = function() {
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
  },

  getOutcomeCohortNames = function() {
    result <- queryDb("SELECT DISTINCT COHORT_NAME FROM @schema.outcome ORDER BY COHORT_NAME")
    return(result$COHORT_NAME)
  },

  getExposureCohortNames = function() {
    result <- queryDb("SELECT DISTINCT COHORT_NAME FROM @schema.target ORDER BY COHORT_NAME")
    return(result$COHORT_NAME)
  },

  getExposureClassNames = function() {
    result <- queryDb("SELECT DISTINCT EXPOSURE_CLASS_NAME FROM @schema.exposure_class ORDER BY EXPOSURE_CLASS_NAME")
    return(result$EXPOSURE_CLASS_NAME)
  },

  getFilteredTableResults = function(benefitThreshold = 0.5,
                                     riskThreshold = 2.0,
                                     pValueCut = 0.05,
                                     filterByMeta = FALSE,
                                     outcomeCohortTypes = c(0, 1, 2),
                                     calibrated = TRUE,
                                     excludeIndications = TRUE,
                                     benefitSelection = c('all', 'most'),
                                     riskSelection = c('none', 'one')) {
    calibrated <- ifelse(calibrated, 1, 0)
    benefitSelection <- paste0("'", paste0(benefitSelection, sep = "'"))
    riskSelection <- paste0("'", paste0(riskSelection, sep = "'"))

    sql <- readr::read_file(system.file("sql/queries/", "mainTable.sql", package = "rewardb"))
    df <- queryDb(
      sql,
      risk = riskThreshold,
      benefit = benefitThreshold,
      p_cut_value = pValueCut,
      exclude_indications = excludeIndications,
      filter_outcome_types = length(outcomeCohortTypes) > 0,
      outcome_types = outcomeCohortTypes,
      risk_selection = riskSelection,
      benefit_selection = benefitSelection,
      calibrated = calibrated,
      show_exposure_classes = appContext$useExposureControls,
      filter_by_meta_analysis = filterByMeta
    )
    return(df)
  },

  getNegativeControls = function() {
    sql <- readr::read_file(system.file("sql/export/", "negativeControls.sql", package = "rewardb"))
    df <- queryDb(sql)
    return(df)
  },

  getMappedAssociations = function() {
    sql <- readr::read_file(system.file("sql/export/", "mappedIndications.sql", package = "rewardb"))
    df <- queryDb(sql)
    return(df)
  }

)