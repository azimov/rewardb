#'
#' @param connection DatabaseConnector connection
#' @outcomeIds condition concept ids
#' @targetIds drug concept ids
#' @schema CEM schema
#' @vocabularySchema schema for vocabulary used
#' @summaryTable schma for summary of CEM results - probably matrix summary
getMappedEvidenceFromCem <- function(
  connection,
  outcomeIds,
  drugIds,
  schema,
  vocabularySchema,
  summaryTable = "matrix_summary"
) {

  outcomeIdsDf <- data.frame(condition_concept_id = outcomeIds)
  # Uses temp tables as these can, and will, be large lists of concepts
  DatabaseConnector::insertTable(connection, "#outcome_nc_tmp", outcomeIdsDf, tempTable = TRUE)
  DatabaseConnector::insertTable(connection, "#target_nc_tmp", drugIds, tempTable = TRUE)

  sql <- SqlRender::readSql(system.file("sql/queries", "cemSummary.sql", package = "rewardb"))
  # First, method of mapping evidence at the normal level
  evidenceConcepts <- DatabaseConnector::renderTranslateQuerySql(
    connection,
    sql,
    schema = schema,
    vocab_schema = vocabularySchema,
    summary_table = summaryTable
  )

  outcomeCounts <- data.frame(
    outcomeIds = outcomeIds,
    counts = lapply(outcomeIds, function(id) { sum(evidenceConcepts$CONDITION_CONCEPT_ID == id) })
  )
  # TODO Map counts to cohorts so cohort sum determines if step up is made - not the cohort
  if (nrow(outcomeCounts[outcomeCounts$counts == 0,])) {
    # Only if we can't map evidence, go up to the level of parent of concept id
    sql <- SqlRender::readSql(system.file("sql/queries", "cemSummaryParents.sql", package = "rewardb"))
    parentLevelEvidenceConcepts <- DatabaseConnector::renderTranslateQuerySql(
      connection,
      sql,
      schema = schema,
      summary_table = summaryTable,
      outcome_concepts_of_interest = outcomeCounts[outcomeCounts$counts == 0,]$outcomeIds
    )
    evidenceConcepts <- rbind(evidenceConcepts, parentLevelEvidenceConcepts)
  }

  return(evidenceConcepts)
}

#'
#' @param connection DatabaseConnector connection
#' @targetCohortTable condition concept ids
#' @outcomeCohortTable drug concept ids
#' @schema CEM schema
#' @summaryTable schma for summary of CEM results - probably matrix summary
#'
getMappedControls <- function(
  connection,
  targetCohortTable,
  outcomeCohortTable,
  schema,
  summaryTable = "matrix_summary"
) {
  sql <- SqlRender::readSql(system.file("sql/queries", "negativeControlsCem.sql", package = "rewardb"))
  evidenceConcepts <- DatabaseConnector::renderTranslateQuerySql(
    connection,
    sql,
    schema = schema,
    target_cohort_table = targetCohortTable,
    outcome_cohort_table = outcomeCohortTable,
    summary_table = summaryTable
  )
}

getTargetConcepts <- function(connection, schema, cohortDefinitionTable, targetCohortIds, atlasTargetIds) {
  sql <- "
SELECT cohort_definition_id as target_cohort_id, drug_conceptset_id as target_concept_id
FROM @schema.@target_cohort_table
{@use_subset_targets} ? {WHERE cohort_definition_id IN (@target_cohort_ids)}
UNION
SELECT CUSTOM_EXPOSURE_ID as target_cohort_id, concept_id as target_concept_id
FROM @schema.custom_exposure_concept
{@use_subset_atlas_targets} ? {WHERE CUSTOM_EXPOSURE_ID IN (@atlas_target_cohort_ids)}
  "
  useTargets <- !is.null(targetCohortIds) | !is.null(atlasTargetIds)
  if (is.null(targetCohortIds)) {
    targetCohortIds <- "NULL"
  }
  
  if (is.null(atlasTargetIds)) {
    atlasTargetIds <- "NULL"
  }
    
  res <- DatabaseConnector::renderTranslateQuerySql(
    connection,
    sql,
    schema = schema,
    target_cohort_table = cohortDefinitionTable,
    # select from "WHERE IN (NULL)" is a valid case we want
    target_cohort_ids = targetCohortIds,
    atlas_target_cohort_ids = atlasTargetIds,
    use_subset_targets = useTargets,
    use_subset_atlas_targets = useTargets
  )
}


getOutcomeConcepts <- function(connection, schema, outcomeCohortDefinitionTable, atlasConceptReferenceTable, outcomeCohortIds, atlasOutcomeIds) {
    sql <- "
  SELECT cohort_definition_id as outcome_cohort_id, conceptset_id as outcome_concept_id
  FROM @schema.@outcome_cohort_table
  WHERE conceptset_id != 99999999
  {@use_subset_outcomes} ? {AND cohort_definition_id IN (@outcome_cohort_ids)}
  
  UNION 
  
  SELECT cohort_definition_id as outcome_cohort_id, conceptset_id as outcome_concept_id
  FROM @schema.@outcome_cohort_table
  WHERE conceptset_id != 99999999
  {@use_subset_outcomes} ? {AND cohort_definition_id IN (@outcome_cohort_ids)}
  
  UNION
  
  SELECT COHORT_DEFINITION_ID as outcome_cohort_id, CONCEPT_ID as outcome_concept_id
  FROM @schema.@atlas_outcome_concept_table
  {@use_subset_atlas_outcomes} ? {WHERE cohort_definition_id IN (@atlas_outcome_cohort_ids)}
    "
  useOutcomeIds <- !is.null(outcomeCohortIds) | !is.null(atlasOutcomeIds)
  if (is.null(outcomeCohortIds)) {
    outcomeCohortIds <- "NULL"
  }
  
  if (is.null(atlasOutcomeIds)) {
    atlasOutcomeIds <- "NULL"
  }
    
  res <- DatabaseConnector::renderTranslateQuerySql(
    connection,
    sql,
    schema = schema,
    outcome_cohort_table = outcomeCohortDefinitionTable,
    atlas_outcome_concept_table = atlasConceptReferenceTable,
    # select from "WHERE IN (NULL)" is a valid case we want
    outcome_cohort_ids = outcomeCohortIds,
    atlas_outcome_cohort_ids = atlasOutcomeIds,
    use_subset_outcomes = useOutcomeIds,
    use_subset_atlas_outcomes = useOutcomeIds
  )
}

#' From  outcome and target cohort ids, construct tables that contain their concept ids
getStudyControls <- function (
  connection,
  schema,
  cemSchema,
  cohortDefinitionTable,
  outcomeCohortDefinitionTable,
  atlasConceptReferenceTable,
  targetCohortIds = NULL,
  outcomeCohortIds = NULL,
  atlasOutcomeIds = NULL,
  atlasTargetIds = NULL
) {
  resTargets <- getTargetConcepts(connection, schema, cohortDefinitionTable, targetCohortIds, atlasTargetIds)
  print(nrow(resTargets))
  DatabaseConnector::insertTable(connection, "#target_cohort_table", resTargets, tempTable = TRUE)

  res <- getOutcomeConcepts(connection, schema, outcomeCohortDefinitionTable, atlasConceptReferenceTable, outcomeCohortIds, atlasOutcomeIds)
  print(nrow(res))
  DatabaseConnector::insertTable(connection, "#outcome_cohort_table", res, tempTable = TRUE)

  mappedControls <- getMappedControls(
    connection,
    "#target_cohort_table",
    "#outcome_cohort_table",
    cemSchema
  )

  return(mappedControls)
}