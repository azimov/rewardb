#' @importFrom pool dbPool poolClose
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
    data <- NULL

    tryCatch({
      if (is(dbConn, "Pool")) {
        data <- DatabaseConnector::dbGetQuery(dbConn, sql)
        if (snakeCaseToCamelCase) {
          colnames(data) <- SqlRender::snakeCaseToCamelCase(colnames(data))
        } else {
          colnames(data) <- toupper(colnames(data))
        }
      } else {
        data <- DatabaseConnector::querySql(dbConn, sql, snakeCaseToCamelCase = snakeCaseToCamelCase)
      }
    }, error = function(e, ...) {
      ParallelLogger::logError(e)
      DatabaseConnector::dbExecute(dbConn, "ABORT;")
      stop(e)
    })
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

  getTimeToOutcomeStats = function(treatment, outcome, sourceIds = NULL, tableName = "scc_result", analysisId = 1) {
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

          FROM @schema.@table_name tts
          INNER JOIN @schema.data_source ds ON tts.source_id = ds.source_id
          WHERE target_cohort_id = @treatment AND outcome_cohort_id = @outcome
          AND mean_time_to_outcome is not NULL
          AND analysis_id = @analysis_id
          {@source_ids != ''} ? {AND ds.source_id IN (@source_ids)}",
            analysis_id = analysisId,
            table_name = tableName,
            treatment = treatment,
            outcome = outcome,
            source_ids = sourceIds)
  },

  getTimeOnTreatmentStats = function(treatment, outcome, sourceIds = NULL, tableName = "scc_result", analysisId = 1) {
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
      FROM @schema.@table_name tts
      INNER JOIN @schema.data_source ds ON tts.source_id = ds.source_id
      WHERE target_cohort_id = @treatment AND outcome_cohort_id = @outcome
      AND mean_tx_time is not NULL
      AND analysis_id = @analysis_id
      {@source_ids != ''} ? {AND ds.source_id IN (@source_ids)}",
            analysis_id = analysisId,
            table_name = tableName,
            treatment = treatment,
            outcome = outcome,
            source_ids = sourceIds)
  },

  getDataSourceInfo = function() {
    cacheQuery(paste0(schemaName, "dataSourceInfo"), " SELECT * from @schema.data_source WHERE source_id > 0", snakeCaseToCamelCase = TRUE)
  },

  getDataSources = function() {
    cacheQuery(paste0(schemaName, "dataSources"), "SELECT source_id, source_name FROM @schema.data_source;")
  }

)