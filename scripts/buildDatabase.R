library(foreach)
library(doParallel)

# This function will change a lot as the method for storing cohorts change
createCohortReferences <- function(conn) {
  sql <- "SELECT TARGET_COHORT_ID, cast(TARGET_COHORT_ID / 1000 AS INT) AS DRUG_CONCEPT_ID FROM TARGET"
  df <- DatabaseConnector::renderTranslateQuerySql(conn, sql)
  DatabaseConnector::dbWriteTable(conn, "target_concept", df, overwrite = TRUE)

  sql <- "SELECT OUTCOME_COHORT_ID, cast(OUTCOME_COHORT_ID / 100 AS INT) AS CONDITION_CONCEPT_ID FROM OUTCOME"
  df <- DatabaseConnector::renderTranslateQuerySql(conn, sql)
  DatabaseConnector::dbWriteTable(conn, "outcome_concept", df, overwrite = TRUE)
}


createExposureClasses <- function(pdwConnection, connection) {
  dtf <- DatabaseConnector::renderTranslateQuerySql(pdwConnection, "SELECT * FROM scratch.dkern2.Rxnorm_ATC_map_unique")
  
  DatabaseConnector::dbWriteTable(connection, "exposure_classes", dtf)
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
  
  # TODO: Import exposure classes for medications
  if ("EXPOSURE_CLASS" %in% names(dataFrame)) {
    exposureClasses <- dplyr::distinct(dataFrame[, c("TARGET_COHORT_ID", "EXPOSURE_CLASS")])
    DatabaseConnector::dbWriteTable(conn, "treatment_classes", exposureClasses, overwrite = overwrite)
  }
  # Add table for cohort - concept id references
  createCohortReferences(conn)
}

buildFromConfig <- function(appContext, ignoreCache = FALSE) {
  connection <- DatabaseConnector::connect(appContext$connectionDetails)
  pdwConnection <- DatabaseConnector::connect(connectionDetails = appContext$resultsDatabase$cdmDataSource)
  
  fullResultsCachePath = paste0(".full_results_", appContext$short_name, ".csv")
  
  if(ignoreCache || !file.exists(fullResultsCachePath)) {
    if (is.null(appContext$outcome_concept_ids)) {
      print("extracting exposure results")
      targetIds <- appContext$target_concept_ids
      fullResults <- activesurveillancedev::getFullResultsSubsetTreatments(connection = pdwConnection,
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
      fullResults <- activesurveillancedev::getFullResultsSubsetOutcomes(connection = pdwConnection,
                                                                         resultsDatabaseSchema = appContext$resultsDatabase$schema,
                                                                         cohortDefinitionTable = appContext$resultsDatabase$cohortDefinitionTable,
                                                                         outcomeCohortDefinitionTable = appContext$resultsDatabase$outcomeCohortDefinitionTable,
                                                                         customOutcomeCohortList = outcomeIds,
                                                                         asurvResultsTable = appContext$resultsDatabase$asurvResultsTable)
    } else {
      print("ERROR: check config - cannot create dataset without specifying either subset of outcomes or targets")
      return(NULL)
    }
    #  Cache results
    write.csv(fullResults, fullResultsCachePath, row.names=FALSE)
  } else {
    print(paste("using cached file", fullResultsCachePath))
    fullResults <- read.csv(fullResultsCachePath)
  }
  
  exportToDashboarDatabase(fullResults, connection, overwrite = TRUE)
  createExposureClasses(pdwConnection, connection)

  DatabaseConnector::disconnect(pdwConnection)
  DatabaseConnector::disconnect(connection)
}