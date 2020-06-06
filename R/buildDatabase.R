preComputeSliderCounts <- function(connection) {
  benefitsql <- "
  SELECT OUTCOME_COHORT_ID, TARGET_COHORT_ID, count(*) AS thresh_count
    FROM results WHERE RR <= @threshold AND p_value < @pthreshold
  GROUP BY OUTCOME_COHORT_ID, TARGET_COHORT_ID
  "
  for (t in 1:9 * 0.1) {
    data <- DatabaseConnector::renderTranslateQuerySql(connection, benefitsql, threshold = t, pthreshold = 0.05)
    tableName <- stringr::str_replace_all(paste0("BEN_TH", t), "[.]", "_")
    DatabaseConnector::dbWriteTable(connection, tableName, data, overwrite = TRUE)
  }
  
  benefitsql <- "
  SELECT OUTCOME_COHORT_ID, TARGET_COHORT_ID, count(*) AS thresh_count
    FROM results WHERE RR >= @threshold AND p_value < @pthreshold
  GROUP BY OUTCOME_COHORT_ID, TARGET_COHORT_ID
  "
  for (t in 11:25 * 0.1) {
    data <- DatabaseConnector::renderTranslateQuerySql(connection, benefitsql, threshold = t, pthreshold = 0.05)
    tableName <- stringr::str_replace_all(paste0("RISK_TH", t), "[.]", "_")
    DatabaseConnector::dbWriteTable(connection, tableName, data, overwrite = TRUE)
  }
}



extractTargetCohortNames <- function (appContext) {
  sql <- " SELECT t.cohort_definition_id AS target_cohort_id, t.cohort_definition_id/1000 AS drug_concept_id, t.short_name AS cohort_name
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

  DatabaseConnector::dbAppendTable(appContext$connection, "target", nameSet)
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
    nameSet <- DatabaseConnector::renderTranslateQuerySql(appContext$cdmConnection, sql, 
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
  
  DatabaseConnector::dbAppendTable(appContext$connection, "outcome", nameSet)
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
  if(!is.null(targetCohorts)) {
    sql <- paste(sql, "AND target_cohort_id in (@target_cohort_ids)")
  }

  resultSet <- DatabaseConnector::renderTranslateQuerySql(appContext$cdmConnection, sql, 
                                                           target_cohort_ids =targetCohorts,
                                                           scca_results = appContext$resultsDatabase$asurvResultsTable,
                                                           results_database_schema = appContext$resultsDatabase$schema)
  
  resultSet$study_design <- "scc"
  DatabaseConnector::dbAppendTable(appContext$connection, "result", resultSet)
}


createTables <- function (appContext) {
  pathToSqlFile <- system.file("sql/create", "reward_schema.sql", package = "rewardb")
  sql <- SqlRender::readSql(pathToSqlFile)
  DatabaseConnector::executeSql(appContext$connection, sql = sql)
}


buildFromConfig <- function(appContext) {
  createTables(appContext)
  extractResultsSubset(appContext)
  extractTargetCohortNames(appContext)
  extractOutcomeCohortNames(appContext)
}