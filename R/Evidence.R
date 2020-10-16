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
  vocabularySchema,
  summaryTable = "matrix_summary"
) {
  sql <- SqlRender::readSql(system.file("sql/queries", "negativeControlsCem.sql", package = "rewardb"))
  evidenceConcepts <- DatabaseConnector::renderTranslateQuerySql(
    connection,
    sql,
    schema = schema,
    vocab_schema = vocabularySchema,
    target_cohort_table = targetCohortTable,
    outcome_cohort_table = outcomeCohortTable,
    summary_table = summaryTable
  )
}

getTargetConcepts <- function(connection, schema, targetCohortIds) {
  sql <- "
SELECT cohort_definition_id as target_cohort_id, drug_conceptset_id as target_concept_id, ATC_flg as is_atc_4
FROM @schema.cohort_definition
WHERE ATC_flg IN (0, 1)
{@use_subset_targets} ? {AND cohort_definition_id IN (@target_cohort_ids)}
UNION
SELECT cohort_definition_id as target_cohort_id, concept_id as target_concept_id, 0 as is_atc_4
FROM @schema.custom_exposure_concept
{@use_subset_targets} ? {WHERE cohort_definition_id IN (@target_cohort_ids)}
  "
  useTargets <- !is.null(targetCohortIds)
  res <- DatabaseConnector::renderTranslateQuerySql(
    connection,
    sql,
    schema = schema,
    target_cohort_ids = targetCohortIds,
    use_subset_targets = useTargets
  )
}


getOutcomeConcepts <- function(connection, schema, outcomeCohortIds) {
  sql <- "
SELECT cohort_definition_id as outcome_cohort_id, conceptset_id as outcome_concept_id
FROM @schema.outcome_cohort_definition
WHERE OUTCOME_TYPE IN (0,1)
{@use_subset_outcomes} ? {AND cohort_definition_id IN (@outcome_cohort_ids)}

UNION

SELECT cohort_definition_id as outcome_cohort_id, conceptset_id as outcome_concept_id
FROM @schema.outcome_cohort_definition
WHERE OUTCOME_TYPE IN (0,1)
{@use_subset_outcomes} ? {AND cohort_definition_id IN (@outcome_cohort_ids)}

UNION

SELECT COHORT_DEFINITION_ID as outcome_cohort_id, CONCEPT_ID as outcome_concept_id
FROM @schema.atlas_concept_reference
{@use_subset_outcomes} ? {WHERE cohort_definition_id IN (@outcome_cohort_ids)}
    "
  useOutcomeIds <- !is.null(outcomeCohortIds)
  res <- DatabaseConnector::renderTranslateQuerySql(
    connection,
    sql,
    schema = schema,
    # select from "WHERE IN (NULL)" is a valid case we want
    outcome_cohort_ids = outcomeCohortIds,
    use_subset_outcomes = useOutcomeIds
  )
}

#' From  outcome and target cohort ids, construct tables that contain their concept ids

getStudyControls <- function(
  connection,
  schema,
  cemSchema,
  vocabularySchema,
  targetCohortIds = NULL,
  outcomeCohortIds = NULL
) {
  targetConceptMapping <- getTargetConcepts(connection, schema, targetCohortIds)
  DatabaseConnector::insertTable(connection, "#target_cohort_table", targetConceptMapping, tempTable = TRUE)

  outcomeConceptMapping <- getOutcomeConcepts(connection, schema, outcomeCohortIds)
  DatabaseConnector::insertTable(connection, "#outcome_cohort_table", outcomeConceptMapping, tempTable = TRUE)

  mappedControls <- getMappedControls(
    connection,
    "#target_cohort_table",
    "#outcome_cohort_table",
    cemSchema,
    vocabularySchema
  )

  if (is.null(targetCohortIds)) {
    mappedCounts <- data.frame(
      outcome_cohort_id = outcomeCohortIds,
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
        vocab_schema = vocabularySchema,
        summary_table = summaryTable,
        outcome_cohort_table = "#outcome_cohort_table",
        target_cohort_table = "#target_cohort_table"
      )

      mappedControls <- rbind(mappedControls, parentLevelEvidenceConcepts)
    }
  }

  return(mappedControls)
}