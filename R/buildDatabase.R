# This function will change a lot as the method for storing cohorts change
createCohortReferences <- function(conn) {
  sql <- "SELECT TARGET_COHORT_ID, cast(TARGET_COHORT_ID / 1000 AS INT) AS TARGET_CONCEPT_ID FROM TARGET"
  df <- DatabaseConnector::renderTranslateQuerySql(conn, sql)
  DatabaseConnector::dbWriteTable(conn, "target_concept", df, overwrite = TRUE)

  sql <- "SELECT OUTCOME_COHORT_ID, cast(OUTCOME_COHORT_ID / 100 AS INT) AS CONDITION_CONCEPT_ID FROM OUTCOME"
  df <- DatabaseConnector::renderTranslateQuerySql(conn, sql)
  DatabaseConnector::dbWriteTable(conn, "outcome_concept", df, overwrite = TRUE)
}

createExposureClasses <- function(connection) {
  # "SELECT CONCEPT_ID, ATC1_CONCEPT_NAME, ATC3_CONCEPT_NAME FROM scratch.dkern2.Rxnorm_ATC_map_unique"
  dtf <- read.csv("extra/concept_classes.csv")
  DatabaseConnector::dbWriteTable(connection, "exposure_classes", dtf, overwrite = TRUE)
}

buildDataSources <- function(connection) {
  DatabaseConnector::dbWriteTable(connection, "data_sources", read.csv("extra/databases.csv"), overwrite = TRUE)
}

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

exportToDashboarDatabase <- function(dataFrame, conn, overwrite = FALSE) {
  resultsColumns <- c("OUTCOME_COHORT_ID", "TARGET_COHORT_ID", "SOURCE_ID", "C_AT_RISK", "C_CASES", "C_PT", "LB_95",
                      "P_VALUE", "RR", "T_AT_RISK", "T_CASES", "T_PT", "UB_95")
  DatabaseConnector::dbWriteTable(conn, "results", dataFrame[, resultsColumns], overwrite = overwrite)

  outcomesRefs <- dplyr::distinct(dataFrame[, c("OUTCOME_COHORT_ID", "OUTCOME_COHORT_NAME")])
  targetRefs <- dplyr::distinct(dataFrame[, c("TARGET_COHORT_ID", "TARGET_COHORT_NAME")])

  DatabaseConnector::dbWriteTable(conn, "outcome", outcomesRefs, overwrite = overwrite)
  DatabaseConnector::dbWriteTable(conn, "target", targetRefs, overwrite = overwrite)

  # Add table for cohort - concept id references
  createCohortReferences(conn)
}


extractTargetCohortNames <- function (connection, cdmConnection, targetCohortIds=NULL) {
  sql <- " SELECT t.cohort_definition_id AS target_cohort_id, t.cohort_definition_id/1000 AS drug_concept_id, t.short_name AS target_cohort_name
      from @results_database_schema.@cohort_definition_table t";

  if (!is.null(tartgetCohortIds)) {
    sql <- paste(sql, "WHERE t.cohort_definition_id IN (@target_cohort_ids)")
  }
  nameSet <- DatabaseConnector::renderTranslateExecuteSql(cdmConnection, sql, target_cohort_id = targetCohortIds)

  DatabaseConnector::dbAppendTable(connection, "target", nameSet)
}

extractResultsSubset <- function(connection, cdmConnection, targetIds=NULL, outcomeIds=NULL){

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
      scca.se_log_rr
      scca.p_value
      from @results_database_schema.@scca_results scca
      WHERE scca.log_rr IS NOT NULL
  "

  if(!is.null(targetIds)) {
    sql <- paste(sql, "AND target_id in (@target_cohort_ids")
  }

  if(!is.null(outcomeCohortIds)) {
    sql <- paste(sql, "AND outcome_chort_id in (@outcome_cohort_ids")
  }

  resultsSet <- DatabaseConnector::renderTranslateQuerySql(cdmConnection, sql, target_cohort_ids=targetIds, outcomeIds=outcomeIds)
  DatabaseConnector::dbAppendTable(connection, "result", resultsSet)
}


buildFromConfig <- function(appContext, ignoreCache = FALSE) {
  if (appContext$development_db) {
    if (is.null(appContext$outcome_concept_ids)) {
      fullResults <- read.csv("extra/devTargetSubset.csv")
    }
    else {
      fullResults <- read.csv("extra/devOutcomeSubset.csv")
    }

  } else {
    fullResultsCachePath = paste0(".full_results_", appContext$short_name, ".csv")
    
    if(ignoreCache || !file.exists(fullResultsCachePath)) {
      if (is.null(appContext$outcome_concept_ids)) {
        print("extracting exposure results")
        targetIds <- appContext$target_concept_ids

        fullResults <- rewardb::getFullResultsSubsetTreatments(connection = appContext$cdmConnection,
                                                                             resultsDatabaseSchema = appContext$resultsDatabase$schema,
                                                                             cohortDefinitionTable = appContext$resultsDatabase$cohortDefinitionTable,
                                                                             outcomeCohortDefinitionTable = appContext$resultsDatabase$outcomeCohortDefinitionTable,
                                                                             drugIngredientConceptList = targetIds,
                                                                             asurvResultsTable = appContext$resultsDatabase$asurvResultsTable)
      } else if (!is.null(appContext$outcome_concept_ids)) {
        outcomeIds <- append(appContext$outcome_concept_ids * 100, appContext$outcome_concept_ids * 100 + 1)
    
        if (!is.null(appContext$custom_outcome_cohort_ids)) {
          outcomeIds <- append(outcomeIds, appContext$custom_outcome_cohort_ids)
        }
    
        print("extracting outcome results")
        fullResults <- rewardb::getFullResultsSubsetOutcomes(connection = appContext$cdmConnection,
                                                                           resultsDatabaseSchema = appContext$resultsDatabase$schema,
                                                                            cohortDefinitionTable = appContext$resultsDatabase$cohortDefinitionTable,
                                                                           outcomeCohortDefinitionTable = appContext$resultsDatabase$outcomeCohortDefinitionTable,
                                                                           customOutcomeCohortList = outcomeIds,
                                                                           asurvResultsTable = appContext$resultsDatabase$asurvResultsTable)
      } else {
        stop("ERROR: check config - cannot create dataset without specifying either subset of outcomes or targets")
      }
      #  Cache results
      write.csv(fullResults, fullResultsCachePath, row.names=FALSE)
    } else {
      print(paste("using cached file", fullResultsCachePath))
      fullResults <- read.csv(fullResultsCachePath)
    }
  }
  exportToDashboarDatabase(fullResults, connection, overwrite = TRUE)
  createExposureClasses(appContext$connection)
  buildDataSources(appContext$connection)
  preComputeSliderCounts(appContext$connection)
}