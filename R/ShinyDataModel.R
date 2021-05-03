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
      initializeConnection()
      on.exit(model$closeConnection())
    }

    if (is.null(schemaName)) {
      warning("Schema name has not been set, call setSchemaName")
    }

    sql <- SqlRender::render(query, schema = schemaName, warnOnMissingParameters = FALSE, ...)
    sql <- SqlRender::translate(sql, targetDialect = "postgresql")


    if (is(dbConn, "Pool")) {
      data <- DatabaseConnector::dbGetQuery(dbConn, sql)
      if (snakeCaseToCamelCase) {
        colnames(data) <- SqlRender::snakeCaseToCamelCase(colnames(data))
      } else {
        colnames(data) <- toupper(colnames(data))
      }
    } else {
      tryCatch({
        data <- DatabaseConnector::querySql(dbConn, sql, snakeCaseToCamelCase = snakeCaseToCamelCase)
      }, error = function(e, ...) {
        ParallelLogger::logError(e)
        DatabaseConnector::dbExecute(dbConn, "ABORT;")
      })
    }
    return(data)
  },

  cacheQuery = function(cacheKey, ...) {
    cacheDir <- config$cacheDir
    if (is.null(cacheDir)) {
      cacheDir <- tempdir()
    } else if (!dir.exists(cacheDir)) {
      dir.create(cacheDir)
    }

    cacheFile <- file.path(cacheDir, paste0(cacheKey, ".rds"))

    if (file.exists(cacheFile)) {
      return(readRDS(cacheFile))
    }

    result <- queryDb(...)

    if (is.data.frame(result)) {
      saveRDS(result, file = cacheFile)
    }
    return(result)
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

  getTimeToOutcomeStats = function(treatment, outcome, sourceIds = NULL) {
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
          WHERE target_cohort_id = @treatment AND outcome_cohort_id = @outcome
          {@source_ids != ''} ? {AND ds.source_id IN (@source_ids)}",
            treatment = treatment,
            outcome = outcome,
            source_ids = sourceIds)
  },

  getTimeOnTreatmentStats = function(treatment, outcome, sourceIds = NULL) {
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
      WHERE target_cohort_id = @treatment AND outcome_cohort_id = @outcome
      {@source_ids != ''} ? {AND ds.source_id IN (@source_ids)}",
            treatment = treatment,
            outcome = outcome,
            source_ids = sourceIds)
  },

  getDataSourceInfo = function() {
    queryDb(" SELECT * from @schema.data_source WHERE source_id > 0", snakeCaseToCamelCase = TRUE)
  },

  getDataSources = function() {
    queryDb("SELECT source_id, source_name FROM @schema.data_source;")
  }

)

#' Dashboard models
DashboardDbModel <- setRefClass("DashboardDbModel", contains = "DbModel")
DashboardDbModel$methods(
  initialize = function(...) {
    callSuper(...)
    setSchemaName(config$short_name)
  },

  getExposureControls = function(outcomeIds, minCohortSize = 10, sourceIds = NULL) {
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

  getOutcomeControls = function(targetIds, minCohortSize = 10, source_ids = NULL) {
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

  getMetaAnalysisTable = function(exposureId, outcomeId, calibrationType = NULL, sourceIds = NULL) {
    sql <- readr::read_file(system.file("sql/queries/", "getTargetOutcomeRowsGrouped.sql", package = "rewardb"))
    return(queryDb(sql, treatment = exposureId, outcome = outcomeId))
  },

  getForestPlotTable = function(exposureId, outcomeId, calibrated, calibrationType = NULL, sourceIds = NULL) {
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

  getFullDataSet = function(calibrated = c(0, 1)) {
    sql <- readr::read_file(system.file("sql/export/", "fullDashboardData.sql", package = "rewardb"))
    df <- queryDb(sql, calibrated = calibrated, show_exposure_classes = config$useExposureControls)
    return(df)
  },

  getOutcomeType = function(outcomeId) {
    res <- queryDb("SELECT type_id FROM @schema.outcome where outcome_cohort_id = @outcome", outcome = outcomeId)
    return(res$TYPE_ID[[1]])
  },

  getExposureOutcomeRows = function(exposure, outcome, calibrated) {
    sql <- readr::read_file(system.file("sql/queries/", "getTargetOutcomeRows.sql", package = "rewardb"))
    queryDb(sql, treatment = exposure, outcome = outcome, calibrated = calibrated)
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

  getOutcomeCohorts = function() {
    sql <- "
    SELECT cd.* FROM @schema.outcome_cohort_definition cd
    INNER JOIN (
      SELECT outcome_cohort_id, count(*) FROM @schema.scc_result sr
      WHERE t_cases + c_cases > 10
      GROUP BY outcome_cohort_id
    ) sq ON sq.outcome_cohort_id = cd.cohort_definition_id
    ORDER BY cohort_definition_name"

    result <- cacheQuery("outcomeCohortsCounted", sql, snakeCaseToCamelCase = TRUE)
  },

  getExposureCohorts = function() {
    sql <- "
    SELECT cd.* FROM @schema.cohort_definition cd
    INNER JOIN (
      SELECT target_cohort_id, count(*) FROM @schema.scc_result sr
      WHERE t_cases + c_cases > 10
      GROUP BY target_cohort_id
    ) sq ON sq.target_cohort_id = cd.cohort_definition_id
    ORDER BY cohort_definition_name"

    result <- cacheQuery("exposureCohortsCounted", sql, snakeCaseToCamelCase = TRUE)
  },

  getOutcomeNullDistributions = function(exposureId, analysisId, outcomeType = 0, sourceIds = NULL) {
    sql <- "
    SELECT * FROM @schema.outcome_null_distributions
    WHERE analysis_id = @analysis_id
    AND target_cohort_id = @exposure_id AND outcome_type = @outcome_type
    {@source_ids != ''} ? {AND source_id IN (@source_ids)}"
    queryDb(sql, exposure_id = exposureId, analysis_id = analysisId, outcome_type = outcomeType, source_ids = sourceIds, snakeCaseToCamelCase = TRUE)
  },

  getExposureNullDistributions = function(outcomeId, analysisId, sourceIds = NULL) {
    sql <- "
    SELECT * FROM @schema.exposure_null_distributions
    WHERE analysis_id = @analysis_id AND outcome_cohort_id = @outcome_id
    {@source_ids != ''} ? {AND source_id IN (@source_ids)}"
    queryDb(sql, outcome_id = outcomeId, analysis_id = analysisId, source_ids = sourceIds, snakeCaseToCamelCase = TRUE)
  },


  getExposureOutcomeData = function(exposureId, outcomeId, analysisId = 1, sourceIds = NULL) {
    sql <- "
     SELECT r.SOURCE_ID,
          ds.SOURCE_NAME,
          r.RR,
          r.SE_LOG_RR,
          CONCAT(round(r.LB_95, 2), ' - ', round(r.UB_95, 2)) AS CI_95,
          r.LB_95,
          r.UB_95,
          r.P_VALUE,
          '-' as Calibrated_RR,
          '-' AS CALIBRATED_CI_95,
          '-' as Calibrated_P_VALUE,
          r.C_AT_RISK,
          r.C_PT,
          r.C_CASES,
          r.T_AT_RISK,
          r.T_PT,
          r.T_CASES,
          '-' as I2
      FROM @schema.scc_result r
      INNER JOIN @schema.data_source ds ON ds.source_id = r.source_id
      WHERE r.target_cohort_id = @exposure_id
      AND r.outcome_cohort_id = @outcome_id
      AND r.analysis_id = @analysis_id
      {@source_ids != ''} ? {AND r.SOURCE_ID IN (@source_ids)}
      ORDER BY r.SOURCE_ID
    "
    queryDb(sql, exposure_id = exposureId, outcome_id = outcomeId, analysis_id = analysisId, source_ids = sourceIds)
  },

  getMetaAnalysisTable = function(exposureId, outcomeId, analysisId = 1, calibrationType = 'outcomes', sourceIds = NULL) {
    checkmate::assert_choice(calibrationType, c('outcomes', 'exposures', 'none'))
    rows <- getExposureOutcomeData(exposureId, outcomeId, analysisId, sourceIds = sourceIds)
    if (nrow(rows)) {
      meta <- metaAnalysis(rows)
      meta$CI_95 <- paste(round(meta$LB_95, 2), "-", round(meta$UB_95, 2))

      meta$SOURCE_NAME <- "Meta Analysis"
      meta$CALIBRATED_RR <- "-"
      meta$CALIBRATED_CI_95 <- "-"
      meta$CALIBRATED_P_VALUE <- "-"
      rows <- rbind(rows, meta)
    }

    nullDists <- data.frame()
    if (calibrationType == 'outcomes') {
      outcomeType <- getOutcomeType(outcomeId)
      nullDists <- getOutcomeNullDistributions(exposureId, analysisId, ifelse(outcomeType == 1, 1, 0), sourceIds = sourceIds)
    }

    if (calibrationType == 'exposures') {
      nullDists <- getExposureNullDistributions(outcomeId, analysisId, sourceIds = sourceIds)
    }

    if (nrow(nullDists)) {
      for (i in 1:nrow(nullDists)) {
        nullDist <- nullDists[i,]
        calibratedRow <- getCalibratedValues(rows[rows$SOURCE_ID == nullDist$sourceId,], nullDist)
        ci95 <- paste(round(exp(calibratedRow$logLb95Rr), 2), "-", round(exp(calibratedRow$logUb95Rr), 2))
        rows[rows$SOURCE_ID == nullDist$sourceId,]$CALIBRATED_P_VALUE <- round(calibratedRow$p, 2)
        rows[rows$SOURCE_ID == nullDist$sourceId,]$CALIBRATED_RR <- round(exp(calibratedRow$logRr), 2)
        rows[rows$SOURCE_ID == nullDist$sourceId,]$CALIBRATED_CI_95 <- ci95
      }
    }


    return(rows)
  },

  getCalibratedValues = function(rows, null) {
    # TODO: checknames of inputs
    nullDist <- null$nullDistMean
    nullDist[2] <- null$nullDistSd
    nullDist[3] <- null$sourceId
    names(nullDist) <- c("mean", "sd", "sourceId")
    class(nullDist) <- c("null")

    errorModel <- EmpiricalCalibration::convertNullToErrorModel(nullDist)
    ci <- EmpiricalCalibration::calibrateConfidenceInterval(log(rows$RR), rows$SE_LOG_RR, errorModel)
    calibratedPValue <- EmpiricalCalibration::calibrateP(nullDist, log(rows$RR), rows$SE_LOG_RR)
    df <- data.frame(logLb95Rr = ci$logLb95Rr, logUb95Rr = ci$logUb95Rr, p = calibratedPValue, logRr = ci$logRr)
    return(df)
  },

  getForestPlotTable = function(exposureId, outcomeId, calibrated, calibrationType = "outcomes", analysisId = 1, sourceIds = NULL) {

    checkmate::assert_choice(calibrationType, c('outcomes', 'exposures', 'none'))
    baseDf <- getMetaAnalysisTable(exposureId, outcomeId, analysisId = analysisId, calibrationType = 'none', sourceIds = sourceIds)

    if (!(1 %in% calibrated) | calibrationType == 'none') {
      return(baseDf)
    }

    nullDists <- data.frame()
    if (calibrationType == "outcomes") {
      outcomeType <- getOutcomeType(outcomeId)
      nullDists <- getOutcomeNullDistributions(exposureId, analysisId, ifelse(outcomeType == 1, 1, 0), sourceIds = sourceIds)
    }

    if (calibrationType == 'exposures') {
      nullDists <- getExposureNullDistributions(outcomeId, analysisId, sourceIds = sourceIds)
    }

    if (!nrow(nullDists)) {
      return(baseDf)
    }

    calibratedRows <- data.frame()
    for (i in 1:nrow(nullDists)) {
      nullDist <- nullDists[i,]
      row <- baseDf[baseDf$SOURCE_ID == nullDist$sourceId,]
      calibratedDt <- getCalibratedValues(row, nullDist)
      row$RR <- exp(calibratedDt$logRr)
      row$LB_95 <- exp(calibratedDt$logLb95Rr)
      row$UB_95 <- exp(calibratedDt$logUb95Rr)
      row$SOURCE_NAME <- paste(row$SOURCE_NAME, "Calibrated")
      calibratedRows <- rbind(calibratedRows, row)
    }

    if (0 %in% calibrated) {
      return(rbind(baseDf, calibratedRows))
    }
    return(calibratedRows)
  },

  getExposureOutcomeDqd = function(exposureOutcomePairs, analysisId = 1) {

    stopifnot("exposureId" %in% colnames(exposureOutcomePairs))
    stopifnot("outcomeId" %in% colnames(exposureOutcomePairs))
    stopifnot(nrow(exposureOutcomePairs) > 0)

    andStrings <- apply(exposureOutcomePairs, 1, function(item) {
      SqlRender::render("(r.target_cohort_id = @exposure_id  AND r.outcome_cohort_id = @outcome_id)",
                        outcome_id = item["outcomeId"],
                        exposure_id = item["exposureId"])
    })
    innerQuery <- paste(andStrings, collapse = " OR ")
    sql <- loadSqlFile("data_quality/getExposureOutcomeDqd.sql")

    queryDb(sql, analysis_id = analysisId, inner_query = innerQuery, snakeCaseToCamelCase = TRUE)
  },

  getOutcomeControls = function(targetIds, sourceIds = NULL, minCohortSize = 10, analysisId = 1) {
    sql <- loadSqlFile("calibration/outcomeCohortNullData.sql")
    return(queryDb(sql,
                   exposure_ids = targetIds,
                   source_ids = sourceIds,
                   cem = config$cemSchema,
                   analysis_id = analysisId,
                   min_cohort_size = minCohortSize,
                   results_schema = config$rewardbResultsSchema,
                   vocabulary_schema = config$vocabularySchema))
  },

  getExposureControls = function(outcomeIds, sourceIds = NULL, minCohortSize = 10, analysisId = 1) {
    sql <- loadSqlFile("calibration/exposureCohortNullData.sql")
    return(queryDb(sql,
                   outcome_ids = outcomeIds,
                   source_ids = sourceIds,
                   cem = config$cemSchema,
                   analysis_id = analysisId,
                   min_cohort_size = minCohortSize,
                   results_schema = config$rewardbResultsSchema,
                   vocabulary = config$vocabularySchema))
  },

  getOutcomeType = function(outcomeId) {
    getFirst("SELECT outcome_type FROM @schema.outcome_cohort_definition ocd WHERE ocd.cohort_definition_id = @outcome_id", outcome_id = outcomeId)
  },

  getExposureOutcomeRows = function(exposure, outcome, calibrated) {
    sql <- readr::read_file(system.file("sql/queries/", "getTargetOutcomeRows.sql", package = "rewardb"))
    queryDb(sql, treatment = exposure, outcome = outcome, calibrated = 0, use_calibration = FALSE, result = 'scc_result')
  }
)
