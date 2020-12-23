DbModel <- setRefClass("DbModel", fields = c("config", "dbConn", "connectionActive", "schemaName"))
DbModel$methods(
  initialize = function(config, initConnection = TRUE, ...) {
    callSuper(...)
    connectionActive <<- FALSE
    config <<- config
    if (initConnection) {
      initializeConnection()
    }
  },

  initializeConnection = function() {
    dbConn <<- DatabaseConnector::connect(connectionDetails = config$connectionDetails)
    connectionActive <<- TRUE
  },

  closeConnection = function() {
    if (!connectionActive) {
      stop("Connection has not be initialized or has been closed. Call initalizeConnection")
    }
    DatabaseConnector::disconnect(dbConn)
    connectionActive <<- FALSE
  },

  setSchemaName = function(name) {
    schemaName <<- name
  },

  queryDb = function(query, ...) {
    if (!connectionActive) {
      stop("Connection has not be initialized or has been closed. Call initalizeConnection")
    }

    if (is.null(schemaName)) {
      warning("Schema name has not been set, call setSchemaName")
    }

    tryCatch({
      df <- DatabaseConnector::renderTranslateQuerySql(dbConn, query, schema = schemaName, warnOnMissingParameters = FALSE, ...)
      return(df)
    },
    error = function(e) {
      ParallelLogger::logError(e)
      closeConnection()
      initializeConnection()
    })
  },
  # Will only work with postgres > 9.4
  tableExists = function(tableName, schema = NULL) {
    schema <- ifelse(is.null(schema), schemaName, schema)
    return(!is.na(queryDb("SELECT to_regclass('@schema.@table');", table = tableName, schema = schema))[[1]])
  },

  getFirst = function (query, ...) {
    df <- queryDb(query, ...)

    if (length(df) == 0) {
      return(NULL)
    }

    row <- df[1,]
    return(row)
  }
)

#' Dashboard models
DashboardDbModel <- setRefClass("DashboardDbModel", contains = "DbModel")
DashboardDbModel$methods(
  initialize = function(...) {
    callSuper(...)
    setSchemaName(config$short_name)
  },

  getExposureControls = function(outcomeIds) {
    return(rewardb::getExposureControls(config, dbConn, outcomeIds))
  },

  getOutcomeControls = function(targetIds) {
    return(rewardb::getOutcomeControls(config, dbConn, targetIds))
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
      show_exposure_classes = config$useExposureControls,
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
  },

  getMetaAnalysisTable = function(exposureId, outcomeId) {
    sql <- readr::read_file(system.file("sql/queries/", "getTargetOutcomeRowsGrouped.sql", package = "rewardb"))
    return(model$queryDb(sql, treatment = exposureId, outcome = outcomeId))
  },

  getForestPlotTable = function(exposureId, outcomeId, calibrated) {
    sql <- readr::read_file(system.file("sql/queries/", "getTargetOutcomeRows.sql", package = "rewardb"))
    table <- model$queryDb(sql, treatment = exposureId, outcome = outcomeId, calibrated = calibrated)
    calibratedTable <- table[table$CALIBRATED == 1,]
    uncalibratedTable <- table[table$CALIBRATED == 0,]

    if (nrow(calibratedTable) & nrow(uncalibratedTable)) {
      calibratedTable$calibrated <- "Calibrated"
      uncalibratedTable$calibrated <- "Uncalibrated"
      uncalibratedTable$SOURCE_NAME <- paste0(uncalibratedTable$SOURCE_NAME, "\n uncalibrated")
      calibratedTable$SOURCE_NAME <- paste0(calibratedTable$SOURCE_NAME, "\n Calibrated")
    }

    table <- rbind(uncalibratedTable[order(uncalibratedTable$SOURCE_ID, decreasing = TRUE),],
                   calibratedTable[order(calibratedTable$SOURCE_ID, decreasing = TRUE),])
    return(table)
  }
)

ReportDbModel <- setRefClass("ReportDbModel", contains = "DbModel")
ReportDbModel$methods(
  initialize = function(...) {
    callSuper(...)
    setSchemaName(config$rewardbResultsSchema)
  },

  getExposureCohort = function(cohortId) {
    getFirst("SELECT * FROM @schema.cohort_definition WHERE cohort_definition_id = @cohort_id", cohort_id = cohortId)
  },

  getOutcomeCohort = function(cohortId) {
    getFirst("SELECT * FROM @schema.outcome_cohort_definition WHERE cohort_definition_id = @cohort_id", cohort_id = cohortId)
  },

  getMetaAnalysisTable = function(exposureId, outcomeId, analysisId = 1) {
    sql <- "
     SELECT r.SOURCE_ID,
          ds.SOURCE_NAME,
          r.RR,
          '-' AS CI_95,
          r.P_VALUE,
          '-' as Calibrated_RR,
          '-' AS CALIBRATED_CI_95,
          '-' as Calibrated_P_VALUE,
          r.C_AT_RISK,
          r.C_PT,
          r.C_CASES,
          r.T_AT_RISK,
          r.T_PT,
          r.T_CASES
      FROM @schema.scc_result r
      INNER JOIN @schema.data_source ds ON ds.source_id = r.source_id
      WHERE r.target_cohort_id = @exposure_id
      AND r.outcome_cohort_id = @outcome_id
      AND r.analysis_id = @analysis_id
      ORDER BY r.SOURCE_ID
    "
    return(model$queryDb(sql, exposure_id = exposureId, outcome_id = outcomeId, analysis_id = analysisId))
  }
)