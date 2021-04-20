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
    if (config$useConnectionPool) {
      dbConn <<- pool::dbPool(
        drv = DatabaseConnector::DatabaseConnectorDriver(),
        dbms = config$connectionDetails$dbms,
        server = config$connectionDetails$server(),
        port = config$connectionDetails$port(),
        user = config$connectionDetails$user(),
        password = config$connectionDetails$password()
      )
    } else {
      dbConn <<- DatabaseConnector::connect(connectionDetails = config$connectionDetails)
    }
    connectionActive <<- TRUE
  },

  closeConnection = function() {
    if (!connectionActive) {
      warning("Connection has not be initialized or has been closed.")
    }

    if (DBI::dbIsValid(dbObj = dbConn)) {
      if (is(dbConn, "Pool")) {
        pool::poolClose(pool = dbConn)
      } else {
        DatabaseConnector::disconnect(dbConn)
      }
    }
    connectionActive <<- FALSE
  },

  setSchemaName = function(name) {
    schemaName <<- name
  },

  queryDb = function(query, ..., snakeCaseToCamelCase = FALSE) {
    if (!connectionActive) {
      stop("Connection has not be initialized or has been closed. Call initalizeConnection")
    }

    if (is.null(schemaName)) {
      warning("Schema name has not been set, call setSchemaName")
    }

    sql <- SqlRender::render(query, schema = schemaName, warnOnMissingParameters = FALSE, ...)
    sql <- SqlRender::translate(sql, targetDialect = "postgresql")

    tryCatch({
      if (is(dbConn, "Pool")) {
        data <- DatabaseConnector::dbGetQuery(dbConn, sql)
        if (snakeCaseToCamelCase) {
          colnames(data)  <- SqlRender::snakeCaseToCamelCase(colnames(data))
        } else {
          colnames(data) <- toupper(colnames(data))
        }
      } else {
        data <- DatabaseConnector::querySql(dbConn, sql, snakeCaseToCamelCase = snakeCaseToCamelCase)
      }
      return(data)
    }, error = function(e) {
      ParallelLogger::logError(e)
      # End current transaction to stop other queries being blocked
      if (is(dbConn, "Pool")) {
        writeLines(sql)
        DatabaseConnector::dbExecute(dbConn, "ABORT;")
      } else {
        DatabaseConnector::executeSql(dbConn, "ABORT;")
      }
    })
  },

  countQuery = function(query, ..., render = TRUE) {
    if (render) {
      query <- SqlRender::render(query, ...)
    }

    res <- queryDb("SELECT count(*) as CNT FROM (@sub_query) AS qur", sub_query = query)
    return(res$CNT)
  },

  # Will only work with postgres > 9.4
  tableExists = function(tableName, schema = NULL) {
    schema <- ifelse(is.null(schema), schemaName, schema)
    return(!is.na(queryDb("SELECT to_regclass('@schema.@table');", table = tableName, schema = schema))[[1]])
  },

  getFirst = function(query, ...) {
    df <- queryDb(query, ...)

    if (length(df) == 0) {
      return(NULL)
    }

    row <- df[1,]
    return(row)
  },

  getTimeToOutcomeStats = function(treatment, outcome) {
    queryDb("
          SELECT
            ds.source_name,
            round(mean_time_to_outcome, 3) as mean,
            round(sd_time_to_outcome, 3) as sd,
            min_time_to_outcome as min,
            p10_time_to_outcome as p10,
            p25_time_to_outcome as p25,
            median_time_to_outcome as median,
            p75_time_to_outcome as p75,
            p90_time_to_outcome as p90,
            max_time_to_outcome as max

          FROM @schema.time_on_treatment tts
          LEFT JOIN @schema.data_source ds ON tts.source_id = ds.source_id
          WHERE target_cohort_id = @treatment AND outcome_cohort_id = @outcome",
            treatment = treatment,
            outcome = outcome
    )
  },

  getTimeOnTreatmentStats = function(treatment, outcome) {
    queryDb("
      SELECT
        ds.source_name,
        round(mean_tx_time, 3) as mean,
        round(sd_tx_time, 3) as sd,
        min_tx_time as min,
        p10_tx_time as p10,
        p25_tx_time as p25,
        median_tx_time as median,
        p75_tx_time as p75,
        p90_tx_time as p90,
        max_tx_time as max
      FROM @schema.time_on_treatment tts
      LEFT JOIN @schema.data_source ds ON tts.source_id = ds.source_id
      WHERE target_cohort_id = @treatment AND outcome_cohort_id = @outcome",
            treatment = treatment,
            outcome = outcome
    )
  },

  getDataSourceInfo = function() {
    queryDb(" SELECT * from @schema.data_source WHERE source_id > 0", snakeCaseToCamelCase = TRUE)
  }
)

#' Dashboard models
DashboardDbModel <- setRefClass("DashboardDbModel", contains = "DbModel")
DashboardDbModel$methods(
  initialize = function(...) {
    callSuper(...)
    setSchemaName(config$short_name)
  },

  getExposureControls = function(outcomeIds, minCohortSize = 10) {
    sql <- "
      SELECT r.*, o.type_id as outcome_type
      FROM @schema.result r
      INNER JOIN @schema.negative_control nc ON (
        r.target_cohort_id = nc.target_cohort_id AND nc.outcome_cohort_id = r.outcome_cohort_id
      )
      INNER JOIN @schema.outcome o ON r.outcome_cohort_id = o.outcome_cohort_id
      WHERE r.OUTCOME_COHORT_ID IN (@outcome_cohort_ids)
      AND r.calibrated = 0
      AND T_CASES >= @min_cohort_size
    "
    return(queryDb(sql, outcome_cohort_ids = outcomeIds, min_cohort_size = minCohortSize))
  },

  getOutcomeControls = function(targetIds, minCohortSize = 10) {
    sql <- "
      SELECT r.*, o.type_id as outcome_type
      FROM @schema.result r
      INNER JOIN @schema.negative_control nc ON (
        r.target_cohort_id = nc.target_cohort_id AND nc.outcome_cohort_id = r.outcome_cohort_id
      )
      INNER JOIN @schema.outcome o ON r.outcome_cohort_id = o.outcome_cohort_id
      AND r.calibrated = 0
      AND T_CASES >= @min_cohort_size
      AND r.target_cohort_id IN (@target_cohort_ids)
    "
    return(queryDb(sql, min_cohort_size = minCohortSize, target_cohort_ids = targetIds))
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

  getFilteredTableResultsQuery = function(benefitThreshold = 0.5,
                                          riskThreshold = 2.0,
                                          pValueCut = 0.05,
                                          requiredBenefitSources = NULL,
                                          filterByMeta = FALSE,
                                          outcomeCohortTypes = c(0, 1, 2),
                                          calibrated = TRUE,
                                          excludeIndications = TRUE,
                                          benefitCount = 1,
                                          riskCount = 0,
                                          targetCohortNames = NULL,
                                          outcomeCohortNames = NULL,
                                          exposureClasses = NULL,
                                          orderByCol = NULL,
                                          ascending = NULL,
                                          limit = NULL,
                                          offset = NULL) {
    calibrated <- ifelse(calibrated, 1, 0)
    filterOutcomes <- length(outcomeCohortTypes) > 0

    sql <- readr::read_file(system.file("sql/queries/", "mainTable.sql", package = "rewardb"))
    query <- SqlRender::render(
      sql,
      risk = riskThreshold,
      benefit = benefitThreshold,
      p_cut_value = pValueCut,
      exclude_indications = excludeIndications,
      filter_outcome_types = filterOutcomes,
      outcome_types = outcomeCohortTypes,
      risk_count = riskCount,
      benefit_count = benefitCount,
      calibrated = calibrated,
      show_exposure_classes = config$useExposureControls,
      filter_by_meta_analysis = filterByMeta,
      outcome_cohort_name_length = length(outcomeCohortNames) > 0,
      outcome_cohort_names = outcomeCohortNames,
      target_cohort_name_length = length(targetCohortNames) > 0,
      target_cohort_names = targetCohortNames,
      exposure_classes = exposureClasses,
      required_benefit_sources = requiredBenefitSources,
      required_benefit_count = length(requiredBenefitSources),
      order_by = orderByCol,
      ascending = ascending,
      limit = limit,
      offset = offset
    )
    return(query)
  },

  getFilteredTableResults = function(...) {
    sql <- getFilteredTableResultsQuery(...)
    queryDb(sql)
  },

  getFilteredTableResultsCount = function(...) {
    sql <- getFilteredTableResultsQuery(...)
    countQuery(sql, render = FALSE)
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
    return(queryDb(sql, treatment = exposureId, outcome = outcomeId))
  },

  getForestPlotTable = function(exposureId, outcomeId, calibrated) {
    sql <- readr::read_file(system.file("sql/queries/", "getTargetOutcomeRows.sql", package = "rewardb"))
    table <- queryDb(sql, treatment = exposureId, outcome = outcomeId, calibrated = calibrated)
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
  },

  getTimeOnTreatmentStats = function(...) {
    if (!tableExists("time_on_treatment")) {
      setSchemaName(config$globalConfig$rewardbResultsSchema)
      on.exit(setSchemaName(config$short_name))
    }
    dt <- callSuper(...)
    return(dt)
  },

  getTimeToOutcomeStats = function(...) {
    if (!tableExists("time_on_treatment")) {
      setSchemaName(config$globalConfig$rewardbResultsSchema)
      on.exit(setSchemaName(config$short_name))
    }
    dt <- callSuper(...)
    return(dt)
  },

  getFullDataSet = function(calibrated = c(0,1)) {
    sql <- readr::read_file(system.file("sql/export/", "fullDashboardData.sql", package = "rewardb"))
    df <- queryDb(sql, calibrated = calibrated, show_exposure_classes = config$useExposureControls)
    return(df)
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
    data <- queryDb(sql, exposure_id = exposureId, outcome_id = outcomeId, analysis_id = analysisId)
    return(data)
  },

  getForestPlotTable = function(exposureId, outcomeId, calibrated) {
    sql <- readr::read_file(system.file("sql/queries/", "getTargetOutcomeRows.sql", package = "rewardb"))
    table <- queryDb(sql, treatment = exposureId, outcome = outcomeId, calibrated = 0, result = 'scc_result', use_calibration = FALSE)
    table <- table[table$CALIBRATED == 0,]
    return(table)
  },

  getExposureOutcomeDqd = function(exposureOutcomePairs, analysisId = 1) {

    stopifnot("exposureId" %in% colnames(exposureOutcomePairs))
    stopifnot("outcomeId" %in% colnames(exposureOutcomePairs))
    stopifnot(nrow(exposureOutcomePairs) > 0)

    andStrings <- apply(exposureOutcomePairs, 1, function (item) {
      SqlRender::render("(r.target_cohort_id = @exposure_id  AND r.outcome_cohort_id = @outcome_id)",
                        outcome_id = item["outcomeId"],
                        exposure_id = item["exposureId"])
    })
    innerQuery <- paste(andStrings, collapse = " OR ")

    sql <- "
    SELECT cd.cohort_definition_name as exposure_cohort,
           ocd.cohort_definition_name as outcome_cohort,
           min(r.RR) as min_rr,
           max(r.RR) as max_rr,
           count(r.source_id) as num_data_sources,
           min(r.P_VALUE) as min_p_value,
           max(r.P_VALUE) as max_p_value,
           min(r.C_AT_RISK) as min_c_at_risk,
           max(r.C_AT_RISK) as max_c_at_risk,
           min(r.C_CASES) as min_c_cases,
           max(r.C_CASES) as max_c_cases,
           min(r.T_AT_RISK) as min_t_at_risk,
           max(r.T_AT_RISK) as max_t_at_risk,
           min(r.T_CASES) as min_t_cases,
           max(r.T_CASES) as max_t_cases
      FROM @schema.scc_result r
      INNER JOIN @schema.data_source ds ON ds.source_id = r.source_id
      INNER JOIN @schema.outcome_cohort_definition ocd on r.outcome_cohort_id = ocd.cohort_definition_id
      INNER JOIN @schema.cohort_definition cd on r.target_cohort_id = cd.cohort_definition_id
      WHERE r.analysis_id = @analysis_id
      AND r.source_id != -99
      AND ( @inner_query )

      group by cd.cohort_definition_name, ocd.cohort_definition_name, cd.cohort_definition_id, ocd.cohort_definition_id
    "

    queryDb(sql, analysis_id = analysisId, inner_query = innerQuery, snakeCaseToCamelCase = TRUE)
  }
)
