#' Map target cohorts to names and exposure classes
#' @param appContext rewardb app context
extractTargetCohortNames <- function (appContext) {
  library(dplyr)
  sql <- "
  SELECT
    t.cohort_definition_id AS target_cohort_id,
    t.short_name AS cohort_name,
    CASE
      WHEN c1.concept_class_id = 'ATC 4th' THEN 1
      ELSE 0
    END AS is_atc_4,
    0 AS is_custom_cohort
  FROM @results_database_schema.@cohort_definition_table t
  INNER JOIN @cdm_vocabulary.concept c1 ON t.cohort_definition_id/1000 = c1.concept_id
  {@fixed_target_cohorts} ? {   WHERE t.cohort_definition_id IN (@target_cohort_ids)}
  {@use_custom_exposure_ids} ? {
    UNION

    SELECT
      custom_exposure_id AS target_cohort_id,
      cohort_name,
      0 AS is_atc_4,
      1 AS is_custom_cohort
    FROM @results_database_schema.custom_exposure
    {@fixed_custom_exposures} ? {   WHERE custom_exposure_id IN (@custom_exposure_ids)}
  }
  ";
  useCustomExposureIds = (length(appContext$target_concept_ids) > 0 & length(appContext$custom_exposure_ids) > 0 ) | length(appContext$target_concept_ids) == 0
  nameSet <- DatabaseConnector::renderTranslateQuerySql(
    appContext$cdmConnection,
    sql,
    cdm_vocabulary = appContext$resultsDatabase$vocabularySchema,
    fixed_target_cohorts = length(appContext$target_concept_ids) > 0,
    use_custom_exposure_ids = useCustomExposureIds,
    fixed_custom_exposures = length(appContext$custom_exposure_ids) > 0,
    target_cohort_ids = appContext$target_concept_ids * 1000,
    custom_exposure_ids = appContext$custom_exposure_ids,
    cohort_definition_table = appContext$resultsDatabase$cohortDefinitionTable,
    results_database_schema = appContext$resultsDatabase$schema
  )

  DatabaseConnector::dbAppendTable(appContext$connection, paste0(appContext$short_name, ".target"), nameSet)

  # Add target concept mappings
  sql <- "
  SELECT
    t.cohort_definition_id AS target_cohort_id,
    t.cohort_definition_id/1000 AS concept_id,
    0 AS is_excluded,
    0 AS include_descendants
  FROM @results_database_schema.@cohort_definition_table t
  {@fixed_target_cohorts} ? {   WHERE t.cohort_definition_id IN (@target_cohort_ids)}

  {@use_custom_exposure_ids} ? {
  UNION
  SELECT
    cec.custom_exposure_id AS target_cohort_id,
    cec.concept_id AS concept_id,
    cec.is_excluded AS is_excluded,
    cec.include_descendants AS include_descendants
  FROM @results_database_schema.custom_exposure_concept cec
  {@fixed_custom_exposures} ? {   WHERE cec.custom_exposure_id IN (@custom_exposure_ids)} }
  "

  conceptMapping <- DatabaseConnector::renderTranslateQuerySql(
    appContext$cdmConnection,
    sql,
    fixed_target_cohorts = length(appContext$target_concept_ids) > 0,
    use_custom_exposure_ids = useCustomExposureIds,
    fixed_custom_exposures = length(appContext$custom_exposure_ids) > 0,
    target_cohort_ids = appContext$target_concept_ids * 1000,
    custom_exposure_ids = appContext$custom_exposure_ids,
    cohort_definition_table = appContext$resultsDatabase$cohortDefinitionTable,
    results_database_schema = appContext$resultsDatabase$schema
  )

 DatabaseConnector::dbAppendTable(appContext$connection, paste0(appContext$short_name, ".target_concept"), conceptMapping)

  # Add exposure classes
  sql <- "SELECT t.cohort_definition_id as target_cohort_id, c.concept_id as exposure_class_id, c.concept_name as exposure_class_name
    FROM @cdm_vocabulary.concept_ancestor ca
    INNER JOIN @cdm_vocabulary.concept c on (ca.ancestor_concept_id = c.concept_id AND c.concept_class_id = 'ATC 3rd')
    INNER JOIN @results_database_schema.@cohort_definition_table t ON (t.cohort_definition_id/1000 = ca.descendant_concept_id)
   {@fixed_target_cohorts} ? {WHERE t.cohort_definition_id IN (@target_cohort_ids)}"


  nameSet <- DatabaseConnector::renderTranslateQuerySql(
    appContext$cdmConnection, sql,
    cdm_vocabulary = appContext$resultsDatabase$vocabularySchema,
    fixed_target_cohorts = length(appContext$target_concept_ids) > 0,
    target_cohort_ids = appContext$target_concept_ids * 1000,
    cohort_definition_table = appContext$resultsDatabase$cohortDefinitionTable,
    results_database_schema = appContext$resultsDatabase$schema
  )

  exposureClasses <- distinct(nameSet, EXPOSURE_CLASS_ID, EXPOSURE_CLASS_NAME)
  DatabaseConnector::dbAppendTable(appContext$connection, paste0(appContext$short_name, ".exposure_class"), exposureClasses)
  exposureClasses <- distinct(nameSet, TARGET_COHORT_ID, EXPOSURE_CLASS_ID)
  DatabaseConnector::dbAppendTable(appContext$connection, paste0(appContext$short_name, ".target_exposure_class"), exposureClasses)
}

#' Map outcome cohorts to names
#' @param appContext rewardb app context
extractOutcomeCohortNames <- function (appContext) {
  sql <- " 
      SELECT 
        o.cohort_definition_id AS outcome_cohort_id,
        o.short_name AS cohort_name, 
        o.cohort_definition_id % 100 as type_id
      from @results_database_schema.@outcome_cohort_definition_table o
      LEFT JOIN @results_database_schema.@atlas_cohort_definition_table acd ON acd.cohort_definition_id = o.cohort_definition_id
      WHERE acd.cohort_definition_id IS NULL
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

#' Map outcomes to concept ids
#' @param appContext rewardb app context
createOutcomeConceptMapping <- function (appContext) {
  sql <- "INSERT INTO @schema.outcome_concept (outcome_cohort_id, condition_concept_id)
    SELECT outcome_cohort_id, outcome_cohort_id/100 AS condition_concept_id
        FROM @schema.outcome WHERE type_id IN (0, 1);"

  DatabaseConnector::renderTranslateExecuteSql(appContext$connection, sql, schema=appContext$short_name)

  sql <- "
  SELECT acd.cohort_definition_id as outcome_cohort_id, acd.concept_id as condition_concept_id
  FROM @results_database_schema.@atlas_cohort_definition_table acd
  WHERE acd.cohort_definition_id IN (@atlas_cohort_ids) AND acd.is_excluded = 0

  UNION

  SELECT acd.cohort_definition_id as outcome_cohort_id, ca.descendant_concept_id as condition_concept_id
  FROM @results_database_schema.@atlas_cohort_definition_table acd
  INNER JOIN @cdm_vocabulary.concept_ancestor ca ON ca.ancestor_concept_id = acd.concept_id
  WHERE acd.include_descendants = 1 AND acd.cohort_definition_id IN (@atlas_cohort_ids) AND acd.is_excluded = 0
  "

  data <- DatabaseConnector::renderTranslateQuerySql(
    appContext$cdmConnection,
    sql,
    cdm_vocabulary = appContext$resultsDatabase$vocabularySchema,
    results_database_schema=appContext$resultsDatabase$schema,
    atlas_cohort_definition_table=appContext$resultsDatabase$atlasCohorts,
    atlas_cohort_ids=appContext$custom_outcome_cohort_ids
  )
  DatabaseConnector::dbAppendTable(appContext$connection, paste(appContext$short_name, ".outcome_concept"), unique(data))
}

#' Takes results from a CDM and puts them in the rewardb schema
#' @param appContext rewardb app context
extractResultsSubset <- function(appContext){

  sql <- "
    select
    DISTINCT
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

  targetCohorts <- append(appContext$target_concept_ids * 1000, appContext$custom_exposure_ids)
  outcomeCohortIds <- append(appContext$outcome_concept_ids * 100, appContext$outcome_concept_ids * 100 + 1)
  resultSet <- DatabaseConnector::renderTranslateQuerySql(appContext$cdmConnection, sql,
                                                           target_cohort_ids = targetCohorts,
                                                           target_cohort_ids_length = length(targetCohorts)  > 0,
                                                           outcome_cohort_ids = append(outcomeCohortIds, appContext$custom_outcome_cohort_ids),
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

#' Using a CEM source finds any evidence related to conditions and exposures
#' This is used for the automated construction of negative control sets and the indication labels
#' TODO: test and improve this logic and move to its own R file
#' @param appContext rewardb app context
addCemEvidence <- function(appContext) {
  cdmConnection <- appContext$cdmConnection

  outcomeIds <- DatabaseConnector::renderTranslateQuerySql(appContext$connection,
                                                           "SELECT DISTINCT condition_concept_id FROM @schema.outcome_concept",
                                                           schema=appContext$short_name)

  targetIds <- DatabaseConnector::renderTranslateQuerySql(appContext$connection,
                                                          "SELECT DISTINCT tc.concept_id AS target_concept_id, t.is_atc_4 FROM @schema.target t
                                                          INNER JOIN @schema.target_concept tc ON tc.target_cohort_id = t.target_cohort_id",
                                                          schema=appContext$short_name)

  DatabaseConnector::insertTable(appContext$cdmConnection, "#outcome_nc_tmp", outcomeIds, tempTable=TRUE)
  DatabaseConnector::insertTable(appContext$cdmConnection, "#target_nc_tmp", targetIds, tempTable=TRUE)

  sql <- SqlRender::readSql(system.file("sql/queries", "cemSummary.sql", package = "rewardb"))
  # First, method of mapping evidence at the normal level
  evidenceConcepts <- DatabaseConnector::renderTranslateQuerySql(
    cdmConnection,
    sql,
    schema = appContext$resultsDatabase$cemSchema,
    vocab_schema = appContext$resultsDatabase$vocabularySchema,
    summary_table = appContext$resultsDatabase$negativeControlTable
  )

  outcomeIds$counts <- lapply(outcomeIds$CONDITION_CONCEPT_ID, function (id) { sum(evidenceConcepts$CONDITION_CONCEPT_ID == id) })

  # TODO Map counts to cohorts so cohort sum determines if step up is made - not the cohort
  if (nrow(outcomeIds[outcomeIds$counts == 0, ])) {
    # Only if we can't map evidence, go up to the level of parent of concept id
    sql <- SqlRender::readSql(system.file("sql/queries", "cemSummaryParents.sql", package = "rewardb"))
    parentLevelEvidenceConcepts <- DatabaseConnector::renderTranslateQuerySql(
      cdmConnection,
      sql,
      schema = appContext$resultsDatabase$cemSchema,
      summary_table = appContext$resultsDatabase$negativeControlTable,
      outcome_concepts_of_interest = outcomeIds[outcomeIds$counts == 0, ]$CONDITION_CONCEPT_ID
    )
    evidenceConcepts <- rbind(evidenceConcepts, parentLevelEvidenceConcepts)
  }
  print(paste("Found ", nrow(evidenceConcepts), "mappings"))

  DatabaseConnector::insertTable(appContext$connection, "#ncc_ids", evidenceConcepts, tempTable=TRUE)

  sql <- "
    INSERT INTO @schema.negative_control (outcome_cohort_id, target_cohort_id)
      SELECT DISTINCT outcome_cohort_id, target_cohort_id
      FROM #ncc_ids ncc
      INNER JOIN @schema.outcome_concept oc ON oc.condition_concept_id = ncc.condition_concept_id
      INNER JOIN @schema.target_concept tc ON tc.concept_id = ncc.ingredient_concept_id
      WHERE ncc.evidence = 0;

    INSERT INTO @schema.positive_indication (outcome_cohort_id, target_cohort_id)
      SELECT DISTINCT outcome_cohort_id, target_cohort_id
      FROM #ncc_ids ncc
      INNER JOIN @schema.outcome_concept oc ON oc.condition_concept_id = ncc.condition_concept_id
      INNER JOIN @schema.target_concept tc ON tc.concept_id = ncc.ingredient_concept_id
      WHERE ncc.evidence = 1;
  "
  # TODO: optimise negative control sets for cohorts
  DatabaseConnector::renderTranslateExecuteSql(appContext$connection, sql, schema=appContext$short_name)
}

#' Perform meta-analysis on data sources
#' @param table data.frame
#' @return data.frame - results of meta analysis
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

#' Runs and saves metanalayis on data
#' @param appContext
performMetaAnalysis <- function(appContext) {
  library(dplyr, warn.conflicts = FALSE)
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

#' Builds a rewardb dashboard for all exposures for subset outcomes or all outcomes for subset exposures
#' Requires the rewardb results to be generated already.
#' This exports the data to a db backend and allows the config file to be used to run the shiny dashboard
#' @param filePath - path to a yaml configuration file used
#' @param calibrateOutcomes - where subset of outcomes is specified
#' @param calibrateTargets - where a subset of target exposures is specified
#' @export
buildFromConfig <- function(filePath, calibrateOutcomes = FALSE, calibrateTargets = FALSE) {
  appContext <- loadAppContext(filePath, createConnection = TRUE, useCdm = TRUE)
  print("Creating schema")
  DatabaseConnector::executeSql(appContext$connection, paste("DROP SCHEMA IF EXISTS", appContext$short_name, "CASCADE;"))
  DatabaseConnector::executeSql(appContext$connection, paste("CREATE SCHEMA ", appContext$short_name))
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
  addCemEvidence(appContext)
  print("Running meta analysis")
  performMetaAnalysis(appContext)
  DatabaseConnector::disconnect(appContext$connection)
  DatabaseConnector::disconnect(appContext$cdmConnection)

  if (calibrateTargets) {
    print("Calibrating targets")
    rewardb::calibrateTargets(appContext, append(appContext$target_concept_ids * 1000, appContext$custom_exposure_ids))
    rewardb::calibrateCustomCohorts(appContext, append(appContext$target_concept_ids * 1000, appContext$custom_exposure_ids))
  }

  if (calibrateOutcomes) {
   print("Calibrating outcomes")
   rewardb::calibrateOutcomes(appContext)
   rewardb::calibrateOutcomesCustomCohorts(appContext)
  }
}