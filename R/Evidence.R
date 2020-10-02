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
SELECT cohort_definition_id as target_cohort_id, drug_conceptset_id as target_concept_id, ATC_flg as is_atc_4
FROM @schema.@target_cohort_table
{@use_subset_targets} ? {WHERE cohort_definition_id IN (@target_cohort_ids)}
UNION
SELECT CUSTOM_EXPOSURE_ID as target_cohort_id, concept_id as target_concept_id, 0 as is_atc_4
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
  useOutcomeIds <- (!is.null(outcomeCohortIds) | !is.null(atlasOutcomeIds))
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

getStudyControls <- function(
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
  targetConceptMapping <- getTargetConcepts(connection, schema, cohortDefinitionTable, targetCohortIds, atlasTargetIds)
  DatabaseConnector::insertTable(connection, "#target_cohort_table", targetConceptMapping, tempTable = TRUE)

  outcomeConceptMapping <- getOutcomeConcepts(connection, schema, outcomeCohortDefinitionTable, atlasConceptReferenceTable, outcomeCohortIds, atlasOutcomeIds)
  DatabaseConnector::insertTable(connection, "#outcome_cohort_table", outcomeConceptMapping, tempTable = TRUE)

  mappedControls <- getMappedControls(
    connection,
    "#target_cohort_table",
    "#outcome_cohort_table",
    cemSchema
  )

  if (is.null(targetCohortIds) & is.null(atlasTargetIds)) {
    outcomeIds <- append(atlasOutcomeIds, outcomeCohortIds)
    mappedCounts <- data.frame(
      outcome_cohort_id = outcomeIds,
      count = sapply(outcomeIds, function(id) { sum(mappedControls$OUTCOME_COHORT_ID == id) })
    )

    zeroIds <- mappedCounts[mappedCounts$count == 0,]$outcome_cohort_id
    zeroCounts <- outcomeConceptMapping[outcomeConceptMapping$OUTCOME_COHORT_ID %in% zeroIds,]

    if (nrow(zeroCounts)) {
      DatabaseConnector::insertTable(connection, "#outcome_cohort_table", zeroCounts, tempTable = TRUE)

      sql <- SqlRender::readSql(system.file("sql/queries", "cemSummaryParents.sql", package = "rewardb"))
      parentLevelEvidenceConcepts <- DatabaseConnector::renderTranslateQuerySql(
        connection,
        sql,
        schema = cemSchema,
        summary_table = summaryTable,
        outcome_cohort_table = "#outcome_cohort_table",
        target_cohort_table = "#target_cohort_table"
      )

      mappedControls <- rbind(mappedControls, parentLevelEvidenceConcepts)
    }
  }

  return(mappedControls)
}