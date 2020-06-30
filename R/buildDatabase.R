extractTargetCohortNames <- function (appContext) {
  sql <- "
  SELECT
    t.cohort_definition_id AS target_cohort_id,
    t.cohort_definition_id/1000 AS target_concept_id,
    t.short_name AS cohort_name,
    CASE
      WHEN c1.concept_class_id = 'ATC 4th' THEN 1
      ELSE 0
    END AS is_atc_4
  FROM @results_database_schema.@cohort_definition_table t
  INNER JOIN @cdm_vocabulary.concept c1 ON t.cohort_definition_id/1000 = c1.concept_id
  {@fixed_target_cohorts} ? {WHERE t.cohort_definition_id IN (@target_cohort_ids)}
      ";


  nameSet <- DatabaseConnector::renderTranslateQuerySql(appContext$cdmConnection, sql,
                                                            cdm_vocabulary = appContext$resultsDatabase$vocabularySchema,
                                                            fixed_target_cohorts = length(appContext$target_concept_ids) > 0,
                                                            target_cohort_ids = appContext$target_concept_ids * 1000,
                                                            cohort_definition_table=appContext$resultsDatabase$cohortDefinitionTable,
                                                            results_database_schema=appContext$resultsDatabase$schema)
 

  DatabaseConnector::dbAppendTable(appContext$connection, paste0(appContext$short_name, ".target"), nameSet)
}

extractOutcomeCohortNames <- function (appContext) {
  sql <- " 
      SELECT 
        o.cohort_definition_id AS outcome_cohort_id,
        o.short_name AS cohort_name, 
        o.cohort_definition_id % 100 as type_id
      from @results_database_schema.@outcome_cohort_definition_table o
      LEFT JOIN @results_database_schema.@atlas_cohort_definition_table acd ON acd.atlas_cohort_id = o.cohort_definition_id
      WHERE acd.atlas_cohort_id IS NULL
      {@outcome_cohort_ids_length} ? {AND o.cohort_definition_id IN (@outcome_cohort_ids)}
    UNION
    -- Only pull specified subset of atlas cohorts
    SELECT 
        o.cohort_definition_id AS outcome_cohort_id,
        o.short_name AS cohort_name, 
        2 as type_id
      FROM @results_database_schema.@outcome_cohort_definition_table o
      WHERE o.cohort_definition_id IN (@custom_outcome_ids)
  ";
  outcome_ids <- append(appContext$outcome_concept_ids * 100, appContext$outcome_concept_ids * 100 + 1)
  # TODO: Join to atlas cohorts and exclude their ids rather than having to always select all custom outcome cohorts

  nameSet <- DatabaseConnector::renderTranslateQuerySql(
      appContext$cdmConnection,
      sql,
      outcome_cohort_ids = outcome_ids,
      atlas_cohort_definition_table=appContext$resultsDatabase$atlasCohorts,
      custom_outcome_ids = appContext$custom_outcome_cohort_ids,
      outcome_cohort_ids_length = length(outcome_ids) > 0,
      outcome_cohort_definition_table=appContext$resultsDatabase$outcomeCohortDefinitionTable,
      results_database_schema=appContext$resultsDatabase$schema
    )
  DatabaseConnector::dbAppendTable(appContext$connection, paste0(appContext$short_name, ".outcome"), nameSet)
}

createOutcomeConceptMapping <- function (appContext) {
  sql <- "INSERT INTO @schema.outcome_concept (outcome_cohort_id, condition_concept_id)
    SELECT outcome_cohort_id, outcome_cohort_id/100 AS condition_concept_id
        FROM @schema.outcome WHERE type_id IN (0, 1);"

  DatabaseConnector::renderTranslateExecuteSql(appContext$connection, sql, schema=appContext$short_name)

  sql <- "SELECT acd.atlas_id as outcome_cohort_id, acd.concept_id as condition_concept_id
  FROM @results_database_schema.@atlas_cohort_definition_table acd
  WHERE acd.atlas_cohort_id IN (@atlas_cohort_ids)

  UNION

  SELECT acd.atlas_id as outcome_cohort_id, ca.descendant_concept_id as condition_concept_id
  FROM @results_database_schema.@atlas_cohort_definition_table acd
  INNER JOIN @cdm_vocabulary.concept_ancestor ca ON ca.ancestor_concept_id = acd.concept_id
  WHERE acd.descendants = 1 AND acd.atlas_cohort_id IN (@atlas_cohort_ids)
  "

  data <- DatabaseConnector::renderTranslateQuerySql(
    appContext$cdmConnection,
    sql,
    cdm_vocabulary = appContext$resultsDatabase$vocabularySchema,
    results_database_schema=appContext$resultsDatabase$schema,
    atlas_cohort_definition_table=appContext$resultsDatabase$atlasCohorts,
    atlas_cohort_ids=appContext$custom_outcome_cohort_ids
  )
  DatabaseConnector::dbAppendTable(appContext$connection, paste(appContext$short_name, ".outcome_concept"), data)
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
      {@target_cohort_ids_length} ? {AND target_cohort_id in (@target_cohort_ids)}
      {@outcome_cohort_ids_length} ? {AND outcome_cohort_id in (@outcome_cohort_ids)}
  "
  
  targetCohorts <- appContext$target_concept_ids * 1000
  outcomeCohortIds <- append(appContext$outcome_concept_ids * 100, appContext$outcome_concept_ids * 100 + 1)
  resultSet <- DatabaseConnector::renderTranslateQuerySql(appContext$cdmConnection, sql, 
                                                           target_cohort_ids = targetCohorts,
                                                           target_cohort_ids_length = length(targetCohorts)  > 0,
                                                           outcome_cohort_ids = outcomeCohortIds,
                                                           outcome_cohort_ids_length = length(outcomeCohortIds)  > 0,
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
                                                           "SELECT DISTINCT condition_concept_id FROM @schema.outcome_concept",
                                                           schema=appContext$short_name)

  targetIds <- DatabaseConnector::renderTranslateQuerySql(appContext$connection,
                                                          "SELECT DISTINCT target_concept_id, is_atc_4 FROM @schema.target",
                                                          schema=appContext$short_name)

  DatabaseConnector::insertTable(appContext$cdmConnection, "#outcome_nc_tmp", outcomeIds, tempTable=TRUE)
  DatabaseConnector::insertTable(appContext$cdmConnection, "#target_nc_tmp", targetIds, tempTable=TRUE)

  sql <- "
  -- Top level conditions that match out hierarchy, we assume that siblings do not have evidence
  SELECT evi.INGREDIENT_CONCEPT_ID AS INGREDIENT_CONCEPT_ID, evi.CONDITION_CONCEPT_ID AS condition_concept_id
    FROM @schema.@summary_table evi
    INNER JOIN #target_nc_tmp ttmp ON ttmp.target_concept_id = evi.ingredient_concept_id
    INNER JOIN #outcome_nc_tmp otmp ON otmp.condition_concept_id = evi.condition_concept_id
    WHERE ttmp.is_atc_4 = 0
    AND evi.evidence_exists = 0

  UNION
  (
    -- Roll up any descendent conditions
    SELECT evi.INGREDIENT_CONCEPT_ID AS INGREDIENT_CONCEPT_ID, otmp.condition_concept_id AS condition_concept_id
      FROM @schema.@summary_table evi
      INNER JOIN #target_nc_tmp ttmp ON ttmp.target_concept_id = evi.ingredient_concept_id
      INNER JOIN @vocab_schema.concept_ancestor ca ON ca.ancestor_concept_id = evi.condition_concept_id
      INNER JOIN #outcome_nc_tmp otmp ON otmp.condition_concept_id = ca.descendant_concept_id
      WHERE ttmp.is_atc_4 = 0
      AND evi.evidence_exists = 0

      EXCEPT
        -- Any descendants that have evidence
        SELECT evi.INGREDIENT_CONCEPT_ID AS INGREDIENT_CONCEPT_ID, otmp.condition_concept_id AS condition_concept_id
          FROM @schema.@summary_table evi
          INNER JOIN #target_nc_tmp ttmp ON ttmp.target_concept_id = evi.ingredient_concept_id
          INNER JOIN @vocab_schema.concept_ancestor ca ON ca.ancestor_concept_id = evi.condition_concept_id
          INNER JOIN #outcome_nc_tmp otmp ON otmp.condition_concept_id = ca.descendant_concept_id
          WHERE ttmp.is_atc_4 = 0
          AND evi.evidence_exists = 1
  )
  UNION
  -- GET all ATC level 4 concept mappings
  (
    SELECT ttmp.target_concept_id AS INGREDIENT_CONCEPT_ID, evi.CONDITION_CONCEPT_ID AS condition_concept_id
      FROM @schema.@summary_table evi
      INNER JOIN #outcome_nc_tmp otmp ON otmp.condition_concept_id = evi.condition_concept_id
      INNER JOIN @vocab_schema.concept_ancestor ca ON ca.descendant_concept_id = evi.ingredient_concept_id
      INNER JOIN #target_nc_tmp ttmp ON ttmp.target_concept_id = ca.ancestor_concept_id
      INNER JOIN @vocab_schema.concept c ON (c.concept_id = evi.ingredient_concept_id AND c.concept_class_id = 'Ingredient')
      WHERE ttmp.is_atc_4 = 1
      AND evi.evidence_exists = 0

      -- Any descendants that have evidence
      EXCEPT
        SELECT ttmp.target_concept_id AS INGREDIENT_CONCEPT_ID, evi.CONDITION_CONCEPT_ID AS condition_concept_id
          FROM @schema.@summary_table evi
          INNER JOIN #outcome_nc_tmp otmp ON otmp.condition_concept_id = evi.condition_concept_id
          INNER JOIN @vocab_schema.concept_ancestor ca ON ca.descendant_concept_id = evi.ingredient_concept_id
          INNER JOIN #target_nc_tmp ttmp ON ttmp.target_concept_id = ca.ancestor_concept_id
          INNER JOIN @vocab_schema.concept c ON (c.concept_id = evi.ingredient_concept_id AND c.concept_class_id = 'Ingredient')
          WHERE ttmp.is_atc_4 = 1
          AND evi.evidence_exists = 1
   )
  "
  negativeControlsConcepts <- DatabaseConnector::renderTranslateQuerySql(
    cdmConnection,
    sql,
    schema = appContext$resultsDatabase$cemSchema,
    vocab_schema = appContext$resultsDatabase$vocabularySchema,
    summary_table = appContext$resultsDatabase$negativeControlTable
  )
  print(paste("Found ", nrow(negativeControlsConcepts), "negative controls"))

  DatabaseConnector::insertTable(appContext$connection, "#ncc_ids", negativeControlsConcepts, tempTable=TRUE)

  sql <- "
    INSERT INTO @schema.negative_control (outcome_cohort_id, target_cohort_id)
      SELECT DISTINCT outcome_cohort_id, target_cohort_id
      FROM #ncc_ids ncc
      INNER JOIN @schema.outcome_concept oc ON oc.condition_concept_id = ncc.condition_concept_id
      INNER JOIN @schema.target t ON t.target_concept_id = ncc.ingredient_concept_id
  "
  DatabaseConnector::renderTranslateExecuteSql(appContext$connection, sql,
                                               schema=appContext$short_name)
}

addCemIndications <- function(appContext) {
  cdmConnection <- appContext$cdmConnection

  outcomeIds <- DatabaseConnector::renderTranslateQuerySql(appContext$connection,
                                                           "SELECT DISTINCT condition_concept_id FROM @schema.outcome_concept",
                                                           schema=appContext$short_name)
  DatabaseConnector::insertTable(appContext$cdmConnection, "#oindtmp", outcomeIds, tempTable=TRUE)
  
  targetIds <- DatabaseConnector::renderTranslateQuerySql(appContext$connection,
                                                          "SELECT DISTINCT target_concept_id, is_atc_4 FROM @schema.target",
                                                          schema=appContext$short_name)
  DatabaseConnector::insertTable(appContext$cdmConnection, "#targetindtmp", targetIds, tempTable=TRUE)

  sql <- "
  SELECT evi.INGREDIENT_CONCEPT_ID AS INGREDIENT_CONCEPT_ID, evi.CONDITION_CONCEPT_ID
  FROM @schema.@summary_table evi
  INNER JOIN #targetindtmp ttmp ON ttmp.target_concept_id = evi.ingredient_concept_id
  INNER JOIN #oindtmp otmp ON otmp.condition_concept_id = evi.condition_concept_id
  WHERE ttmp.is_atc_4 = 0
  AND evi.evidence_exists = 1
  
  UNION
  -- SELECTS THE DESCENDANT CONDITION
  SELECT evi.INGREDIENT_CONCEPT_ID AS INGREDIENT_CONCEPT_ID, otmp.condition_concept_id
  FROM @schema.@summary_table evi
  INNER JOIN @schema.concept_ancestor ca ON ca.ancestor_concept_id = evi.condition_concept_id
  INNER JOIN #targetindtmp ttmp ON ttmp.target_concept_id = evi.ingredient_concept_id
  INNER JOIN #oindtmp otmp ON otmp.condition_concept_id = ca.descendant_concept_id
  WHERE ttmp.is_atc_4 = 0
  AND evi.evidence_exists = 1

  UNION
  -- GET all ATC level 4 concept mappings
  SELECT ttmp.target_concept_id AS INGREDIENT_CONCEPT_ID, evi.CONDITION_CONCEPT_ID
  FROM @schema.@summary_table evi
  INNER JOIN #oindtmp otmp ON otmp.condition_concept_id = evi.condition_concept_id
  INNER JOIN @vocab_schema.concept_ancestor ca ON ca.descendant_concept_id = evi.ingredient_concept_id
  INNER JOIN #targetindtmp ttmp ON ttmp.target_concept_id = ca.ancestor_concept_id
  INNER JOIN @vocab_schema.concept c ON (c.concept_id = evi.ingredient_concept_id AND c.concept_class_id = 'Ingredient')
  WHERE ttmp.is_atc_4 = 1
  AND evi.evidence_exists = 1
  
  "
  negativeControlsConcepts <- DatabaseConnector::renderTranslateQuerySql(
    cdmConnection,
    sql,
    schema = appContext$resultsDatabase$cemSchema,
    vocab_schema = appContext$resultsDatabase$vocabularySchema,
    summary_table = appContext$resultsDatabase$negativeControlTable
  )
  print(paste("Found ", nrow(negativeControlsConcepts), "mapped indications"))

  DatabaseConnector::insertTable(appContext$connection, "#indication_ids", negativeControlsConcepts, tempTable=TRUE)

  DatabaseConnector::renderTranslateExecuteSql(appContext$connection, "DELETE FROM @schema.positive_indication",
                                               schema=appContext$short_name)
  sql <- "
    INSERT INTO @schema.positive_indication (outcome_cohort_id, target_cohort_id)
      SELECT DISTINCT outcome_cohort_id, target_cohort_id
      FROM #indication_ids ncc
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
    SE_LOG_RR = results$seTE.random,
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
    "SELECT * FROM @schema.result WHERE source_id != -99;",
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

buildFromConfig <- function(filePath, calibrateTargets = FALSE, calibrateExposures = FALSE) {
  appContext <- loadAppContext(filePath, createConnection = TRUE, useCdm = TRUE)
  DatabaseConnector::executeSql(appContext$connection, paste("CREATE SCHEMA IF NOT EXISTS", appContext$short_name))
  createTables(appContext)
  
  print("Extracting results from CDM data source")
  extractResultsSubset(appContext)
  print("Extracting cohort names")
  extractTargetCohortNames(appContext)
  print("Extracting otcome cohort names")
  extractOutcomeCohortNames(appContext)
  print("Extracting outcome cohort mapping")
  createOutcomeConceptMapping(appContext)
  print("Adding negative controls from CEM")
  addCemNagativeControls(appContext)
  print("Adding positive indications from CEM")
  addCemIndications(appContext)
  print("Running meta analysis")
  performMetaAnalysis(appContext) 
  DatabaseConnector::disconnect(appContext$connection)
  DatabaseConnector::disconnect(appContext$cdmConnection)

  if (calibrateTargets) {
    print("Calibrating targets")
    rewardb::calibrateTargets(appContext, appContext$target_concept_ids * 1000)
    rewardb::calibrateCustomCohorts(appContext, appContext$target_concept_ids * 1000)
  }

  if (calibrateExposures) {
     rewardb::calibrateOutcomes(appContext)
     rewardb::calibrateOutcomesCustomCohorts(appContext)
  }
}