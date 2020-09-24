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

  drugIdsDf <- data.frame(target_concept_id = drugIds)
  outcomeIdsDf <- data.frame(condition_concept_id = outcomeIds)
  # Uses temp tables as these can, and will, be large lists of concepts
  DatabaseConnector::insertTable(connection, "#outcome_nc_tmp", outcomeIdsDf, tempTable = TRUE)
  DatabaseConnector::insertTable(connection, "#target_nc_tmp", drugIdsDf, tempTable = TRUE)

  sql <- SqlRender::readSql(system.file("sql/queries", "cemSummary.sql", package = "rewardb"))
  # First, method of mapping evidence at the normal level
  evidenceConcepts <- DatabaseConnector::renderTranslateQuerySql(
    connection,
    sql,
    schema = schema,
    vocab_schema = vocabularySchema,
    summary_table = summaryTable
  )

  outcomeIds$counts <- lapply(outcomeIds$CONDITION_CONCEPT_ID, function(id) { sum(evidenceConcepts$CONDITION_CONCEPT_ID == id) })

  # TODO Map counts to cohorts so cohort sum determines if step up is made - not the cohort
  if (nrow(outcomeIds[outcomeIds$counts == 0,])) {
    # Only if we can't map evidence, go up to the level of parent of concept id
    sql <- SqlRender::readSql(system.file("sql/queries", "cemSummaryParents.sql", package = "rewardb"))
    parentLevelEvidenceConcepts <- DatabaseConnector::renderTranslateQuerySql(
      connection,
      sql,
      schema = schema,
      summary_table = summaryTable,
      outcome_concepts_of_interest = outcomeIds[outcomeIds$counts == 0,]$CONDITION_CONCEPT_ID
    )
    evidenceConcepts <- rbind(evidenceConcepts, parentLevelEvidenceConcepts)
  }

  return(evidenceConcepts)
}