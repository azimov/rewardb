extractTargetCohortNames <- function (appContext) {
  sql <- " SELECT t.cohort_definition_id AS target_cohort_id, t.cohort_definition_id/1000 AS target_concept_id, t.short_name AS cohort_name
      from @results_database_schema.@cohort_definition_table t";

  if (!is.null(appContext$target_concept_ids)) {
    sql <- paste(sql, "WHERE t.cohort_definition_id IN (@target_cohort_ids)")
    nameSet <- DatabaseConnector::renderTranslateQuerySql(appContext$cdmConnection, sql, target_cohort_ids = appContext$target_concept_ids * 1000, 
                                                            cohort_definition_table=appContext$resultsDatabase$cohortDefinitionTable,
                                                            results_database_schema=appContext$resultsDatabase$schema)
  } else {
    nameSet <- DatabaseConnector::renderTranslateQuerySql(appContext$cdmConnection, sql, 
                                                            cohort_definition_table=appContext$resultsDatabase$cohortDefinitionTable,
                                                            results_database_schema=appContext$resultsDatabase$schema)
  }

  DatabaseConnector::dbAppendTable(appContext$connection, paste0(appContext$short_name, ".target"), nameSet)
}

extractOutcomeCohortNames <- function (appContext) {
  sql <- " 
      SELECT 
        o.cohort_definition_id AS outcome_cohort_id,
        o.short_name AS cohort_name, 
        o.cohort_definition_id % 100 as type_id
      from @results_database_schema.@outcome_cohort_definition_table o
      WHERE o.cohort_definition_id NOT IN (@custom_outcome_ids)

    UNION

    SELECT 
        o.cohort_definition_id AS outcome_cohort_id,
        o.short_name AS cohort_name, 
        2 as type_id
      FROM @results_database_schema.@outcome_cohort_definition_table o
      WHERE o.cohort_definition_id IN (@custom_outcome_ids)
  ";
  
  # TODO: Join to atlas cohorts and exclude their ids rather than having to always select all custom outcome cohorts
  if (!is.null(appContext$outcome_cohort_ids)) {
    
    outcome_ids <- append(appContext$outcome_cohort_ids * 100, appContext$outcome_cohort_ids * 100 + 1)
    sql <- paste(sql, "AND o.cohort_definition_id IN (@outcome_cohort_ids)")
    nameSet <- DatabaseConnector::renderTranslateQuerySql(
      appContext$cdmConnection, 
      sql, 
      outcome_cohort_ids = outcomes,
      custom_outcome_ids = appContext$custom_outcome_cohort_ids,
      outcome_cohort_definition_table=appContext$resultsDatabase$outcomeCohortDefinitionTable,
      results_database_schema=appContext$resultsDatabase$schema
    )
  } else {
    nameSet <- DatabaseConnector::renderTranslateQuerySql(appContext$cdmConnection, sql, 
                                                            custom_outcome_ids = appContext$custom_outcome_cohort_ids,
                                                            outcome_cohort_definition_table=appContext$resultsDatabase$outcomeCohortDefinitionTable,
                                                            results_database_schema=appContext$resultsDatabase$schema
                                                            )
  }
  
  DatabaseConnector::dbAppendTable(appContext$connection, paste0(appContext$short_name, ".outcome"), nameSet)
}

createOutcomeConceptMapping <- function (appContext) {
  # TODO - somehow map custom cohorts to condition concepts?
  #  Pull from WebApi then what? Could be thousands of concept codes
  sql <- "INSERT INTO @schema.outcome_concept (outcome_cohort_id, condition_concept_id)
    SELECT outcome_cohort_id, outcome_cohort_id/100 AS condition_concept_id
        FROM @schema.outcome WHERE type_id IN (0, 1);"

  DatabaseConnector::renderTranslateExecuteSql(appContext$connection, sql, schema=appContext$short_name)
}

extractResultsSubset <- function(appContext){

  sql <- "
    select
      scca.source_id,
      scca.target_cohort_id,
      scca.outcome_cohort_id,
      scca.t_at_risk,
      scca.t_pt,
      scca.t_cases,
      scca.c_at_risk,
      scca.c_pt,
      scca.c_cases,
      EXP(scca.log_rr) rr,
      scca.lb_95,
      scca.ub_95,
      scca.se_log_rr,
      scca.p_value
      from @results_database_schema.@scca_results scca
      WHERE scca.log_rr IS NOT NULL
  "
  
  targetCohorts <- appContext$target_concept_ids * 1000
  if(length(targetCohorts)) {
    sql <- paste(sql, "AND target_cohort_id in (@target_cohort_ids)")
  }


  outcomeCohortIds <- append(appContext$outcome_concept_ids * 100, appContext$outcome_concept_ids * 100 + 1)
  if (length(outcomeCohortIds)) {
    sql <- paste(sql, "AND outcome_cohort_id in (@outcome_cohort_ids)")
  }

  resultSet <- DatabaseConnector::renderTranslateQuerySql(appContext$cdmConnection, sql, 
                                                           target_cohort_ids = targetCohorts,
                                                           outcome_cohort_ids = outcomeCohortIds,
                                                           scca_results = appContext$resultsDatabase$asurvResultsTable,
                                                           results_database_schema = appContext$resultsDatabase$schema)
  
  resultSet$study_design <- "scc"
  DatabaseConnector::dbAppendTable(appContext$connection,paste0(appContext$short_name, ".result"), resultSet)
}


createTables <- function (appContext) {
  pathToSqlFile <- system.file("sql/create", "reward_schema.sql", package = "rewardb")
  sql <- SqlRender::readSql(pathToSqlFile)
  sql <- SqlRender::render(sql, schema=appContext$short_name)
  DatabaseConnector::executeSql(appContext$connection, sql = sql)
}

addCemNagativeControls <- function(appContext) {
  cdmConnection <- appContext$cdmConnection

  outcomeIds <- DatabaseConnector::renderTranslateQuerySql(appContext$connection,
                                                           "SELECT condition_concept_id FROM @schema.outcome_concept",
                                                           schema=appContext$short_name)$CONDITION_CONCEPT_ID

  targetIds <- DatabaseConnector::renderTranslateQuerySql(appContext$connection,
                                                          "SELECT target_concept_id FROM @schema.target",
                                                          schema=appContext$short_name)$TARGET_CONCEPT_ID

  sql <- "
  SELECT evi.INGREDIENT_CONCEPT_ID, evi.CONDITION_CONCEPT_ID
    FROM @schema.@summary_table evi
    WHERE evi.evidence_exists = 0
    AND evi.ingredient_concept_id IN (@target_ids)
    AND evi.condition_concept_id IN (@outcome_ids)
  "
  negativeControlsConcepts <- DatabaseConnector::renderTranslateQuerySql(
    cdmConnection,
    sql,
    target_ids = targetIds,
    outcome_ids = outcomeIds,
    schema = appContext$resultsDatabase$cemSchema,
    summary_table = appContext$resultsDatabase$negativeControlTable
  )
  print(paste("Found ", nrow(negativeControlsConcepts), "negative controls"))

  DatabaseConnector::dbWriteTable(appContext$connection, paste0(appContext$short_name,".#ncc_ids"), negativeControlsConcepts, overwrite=TRUE)

  sql <- "
    INSERT INTO @schema.negative_control (outcome_cohort_id, target_cohort_id)
      SELECT outcome_cohort_id, target_cohort_id
      FROM @schema.#ncc_ids ncc
      INNER JOIN @schema.outcome_concept oc ON oc.condition_concept_id = ncc.condition_concept_id
      INNER JOIN @schema.target t ON t.target_concept_id = ncc.ingredient_concept_id
  "
  DatabaseConnector::renderTranslateExecuteSql(appContext$connection, sql,
                                               schema=appContext$short_name)
}

metaAnalysis <- function(table) {
  # Compute meta analysis with random effects model
  results <- meta::metainc(
    data = table,
    event.e = T_CASES,
    time.e = T_PT,
    event.c = C_CASES,
    time.c = C_PT,
    sm = "IRR",
    model.glmm = "UM.RS"
  )
  
  row <- data.frame(
    SOURCE_ID = -99,
    T_AT_RISK = sum(table$T_AT_RISK),
    T_PT = sum(table$T_PT),
    T_CASES = sum(table$T_CASES),
    C_AT_RISK = sum(table$C_AT_RISK),
    C_PT = sum(table$C_PT),
    C_CASES = sum(table$C_CASES),
    RR = exp(results$TE.random),
    SE_LOG_RR = log(results$seTE.random),
    LB_95 = exp(results$lower.random),
    UB_95 = exp(results$upper.random),
    P_VALUE = results$pval.random,
    I2 = results$I2
  )
  
  return(row)
}

performMetaAnalysis <- function(appContext) {
  library(dplyr)
  fullResults <- DatabaseConnector::renderTranslateQuerySql(
    appContext$connection,
    "SELECT * FROM @schema.result",
    schema=appContext$short_name
  )

  # For each distinct pair: (target, outcome) get all data sources
  # Run meta analysis
  # Write uncalibrated table
  # Calibrate meta analysis results
  results <- fullResults %>%
    group_by(TARGET_COHORT_ID, OUTCOME_COHORT_ID) %>%
    group_modify(~ metaAnalysis(.x))
  
  results$STUDY_DESIGN <- "scc"
  results$CALIBRATED <- 0

  resultsTable <- paste(appContext$short_name, ".result")
  DatabaseConnector::dbAppendTable(appContext$connection, resultsTable, data.frame(results))
}

buildFromConfig <- function(filePath) {
  appContext <- loadAppContext(filePath, createConnection = TRUE, useCdm = TRUE)
  DatabaseConnector::executeSql(appContext$connection, paste("CREATE SCHEMA IF NOT EXISTS", appContext$short_name))
  createTables(appContext)
  
  print("Extracting results from CDM data source")
  extractResultsSubset(appContext)
  print("Extracting cohort names")
  extractTargetCohortNames(appContext)
  extractOutcomeCohortNames(appContext)
  createOutcomeConceptMapping(appContext)
  print("Adding negative controls from CEM")
  addCemNagativeControls(appContext)
  print("Running meta analysis")
  performMetaAnalysis(appContext) 
  DatabaseConnector::disconnect(appContext$connection)
  DatabaseConnector::disconnect(appContext$cdmConnection)
}