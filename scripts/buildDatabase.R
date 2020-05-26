library(foreach)
library(doParallel)

createCohortReferences <- function(conn) {
  sql <- "SELECT TARGET_COHORT_ID, cast(TARGET_COHORT_ID / 1000 AS INT) AS DRUG_CONCEPT_ID FROM TARGET"
  df <- DatabaseConnector::renderTranslateQuerySql(conn, sql)
  DatabaseConnector::dbWriteTable(conn, "target_concept", df, overwrite = TRUE)

  sql <- "SELECT OUTCOME_COHORT_ID, cast(OUTCOME_COHORT_ID / 100 AS INT) AS CONDITION_CONCEPT_ID FROM OUTCOME"
  df <- DatabaseConnector::renderTranslateQuerySql(conn, sql)
  DatabaseConnector::dbWriteTable(conn, "outcome_concept", df, overwrite = TRUE)
}

exportToDashboarDatabase <- function(dataFrame, conn, overwrite = FALSE) {
  # add heterogenity field - needed for meta analysis
  if (!("I2" %in% names(dataFrame))) {
    dataFrame[, "I2"] <- NA
  }

  resultsColumns <- c("OUTCOME_COHORT_ID", "TARGET_COHORT_ID", "SOURCE_ID", "C_AT_RISK", "C_CASES", "C_PT", "LB_95",
                      "P_VALUE", "RR", "T_AT_RISK", "T_CASES", "T_PT", "UB_95", "I2")
  DatabaseConnector::dbWriteTable(conn, "results", dataFrame[, resultsColumns], overwrite = overwrite)

  outcomesRefs <- dplyr::distinct(dataFrame[, c("OUTCOME_COHORT_ID", "OUTCOME_COHORT_NAME")])
  targetRefs <- dplyr::distinct(dataFrame[, c("TARGET_COHORT_ID", "TARGET_COHORT_NAME")])

  DatabaseConnector::dbWriteTable(conn, "outcome", outcomesRefs, overwrite = overwrite)
  DatabaseConnector::dbWriteTable(conn, "target", targetRefs, overwrite = overwrite)

  # Add table for cohort - concept id references
  createCohortReferences(conn)
}

runMetaAnalysis <- function(dbConn, fullResults, ncores = parallel::detectCores() - 1) {

  exposures <- DatabaseConnector::renderTranslateQuerySql(dbConn, "SELECT DISTINCT(TARGET_COHORT_ID) FROM TARGET")
  outcomes <- DatabaseConnector::renderTranslateQuerySql(dbConn, "SELECT DISTINCT(OUTCOME_COHORT_ID) FROM OUTCOME")

  registerDoParallel(cores = ncores)
  meta_table <- foreach(outcome = outcomes) %:%
    foreach(treatment = exposures, .combine = "rbind") %dopar% {
    sub <- data.frame(subset(fullResults, TARGET_COHORT_ID == treatment & OUTCOME_COHORT_ID == outcome))
    results <- meta::metainc(event.e = T_CASES, time.e = T_PT, event.c = C_CASES, time.c = C_PT,
                             data = sub, sm = "IRR", model.glmm = "UM.RS")
    row <- c(SOURCE_ID = 99, TARGET_COHORT_ID = treatment, OUTCOME_COHORT_ID = outcome,
             T_AT_RISK = sum(sub$T_AT_RISK), T_PT = sum(sub$T_PT), T_CASES = sum(sub$T_CASES),
             C_AT_RISK = sum(sub$C_AT_RISK), C_PT = sum(sub$C_PT), C_CASES = sum(sub$C_CASES),
             RR = exp(results$TE.random), LB_95 = exp(results$lower.random), UB_95 = exp(results$upper.random),
             P_VALUE = results$pval.random, I2 = results$I2)
  }

  DatabaseConnector::dbAppendTable(conn, "results", meta_table)
}

buildFromConfig <- function(appContext) {
  connection <- DatabaseConnector::connect(appContext$connectionDetails)

  pdwConnectionDetails <- DatabaseConnector::createConnectionDetails(appContext$resultsDatabase$cdmDataSource)
  pdwConnection <- DatabaseConnector::connect(connectionDetails = pdwConnectionDetails)

  if (appContext$outcome_concept_ids == NULL) {
    targetIds <- appContext$target_concept_ids * 1000
    fullResults <- activesurveillancedev::getFullResultsSubsetTreatments(connection = pdwConnection,
                                                                         resultsDatabaseSchema = appContext$resultsDatabase$schema,
                                                                         cohortDefinitionTable = appContext$resultsDatabase$cohortDefinitionTable,
                                                                         outcomeCohortDefinitionTable = appContext$resultsDatabase$outcomeCohortDefinitionTable,
                                                                         drugIngredientConceptList = targetIds,
                                                                         asurvResultsTable = appContext$resultsDatabase$asurvResultsTable)
  } else if (appContext$outcome_concept_ids ){
    outcomeIds <- append(appContext$outcome_concept_ids * 100, appContext$outcome_concept_ids * 100 + 1)

    if (appContext$custom_outcome_cohort_ids != NULL) {
      outcomeIds <- append(outcomeIds, appContext$custom_outcome_cohort_ids)
    }
    fullResults <- activesurveillancedev::getFullResultsSubsetOutcomes(connection = pdwConnection,
                                                                         resultsDatabaseSchema = appContext$resultsDatabase$schema,
                                                                         cohortDefinitionTable = appContext$resultsDatabase$cohortDefinitionTable,
                                                                         outcomeCohortDefinitionTable = appContext$resultsDatabase$outcomeCohortDefinitionTable,
                                                                         drugIngredientConceptList = outcomeIds,
                                                                         asurvResultsTable = appContext$resultsDatabase$asurvResultsTable)
  } else {
    print("ERROR: check config - cannot create dataset without specifying either subset of outcomes or targets")
    return(NULL)
  }

  exportToDashboarDatabase(fullResults, conn, overwrite = TRUE)
  runMetaAnalysis <- function(dbConn, fullResults)
  DatabaseConnector::disconnect(connection)
}