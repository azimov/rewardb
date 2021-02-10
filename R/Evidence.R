#' @title
#' Get mapped controlls
#' @description
#' Gets mapped concepts from cem schema
#' @param connection DatabaseConnector connection
#' @param targetCohortIds condition cohort
#' @param outcomeCohortIds drug cohort ids
#' @param schema dashboard schema
#' @param cemSchema CEM schema
#' @param vocabularySchema vocabulary schema
#' @param summaryTable schma for summary of CEM results - probably matrix summary
#' @return data.frame of negative controlls mapped by concept
getMappedControls <- function(
  connection,
  targetCohortIds,
  outcomeCohortIds,
  schema,
  cemSchema,
  vocabularySchema,
  summaryTable = "matrix_summary"
) {
  sql <- SqlRender::readSql(system.file("sql/queries", "negativeControlsCem.sql", package = "rewardb"))
  evidenceConcepts <- DatabaseConnector::renderTranslateQuerySql(
    connection,
    sql,
    schema = schema,
    cem_schema = cemSchema,
    vocab_schema = vocabularySchema,
    target_cohort_ids = targetCohortIds,
    outcome_cohort_ids = outcomeCohortIds,
    summary_table = summaryTable
  )
}

#' @title
#' Get mapped controlls
#' @description
#' From  outcome and target cohort ids, construct tables that contain their concept ids
#' @param connection DatabaseConnector connection
#' @param targetCohortIds condition concept ids
#' @param outcomeCohortIds drug concept ids
#' @param schema dashboard schema
#' @param cemSchema CEM schema
#' @param vocabularySchema vocabulary schema
#' @return data.frame of negative controlls mapped by concept
getStudyControls <- function(
  connection,
  schema,
  cemSchema,
  vocabularySchema,
  targetCohortIds = NULL,
  outcomeCohortIds = NULL
) {

  mappedControls <- getMappedControls(
    connection,
    targetCohortIds,
    outcomeCohortIds,
    schema,
    cemSchema,
    vocabularySchema
  )

  if (length(targetCohortIds)) {
    mappedCounts <- data.frame(
      outcome_cohort_id = outcomeCohortIds,
      count = sapply(outcomeCohortIds, function(id) { sum(mappedControls$OUTCOME_COHORT_ID == id) })
    )

    zeroIds <- mappedCounts[mappedCounts$count == 0,]$outcome_cohort_id
    zeroCounts <- mappedControls[mappedControls$OUTCOME_COHORT_ID %in% zeroIds,]

    if (nrow(zeroCounts)) {
      DatabaseConnector::insertTable(connection, "#outcome_cohort_table", zeroCounts, tempTable = TRUE)

      sql <- SqlRender::readSql(system.file("sql/queries", "cemSummaryParents.sql", package = "rewardb"))
      parentLevelEvidenceConcepts <- DatabaseConnector::renderTranslateQuerySql(
        connection,
        sql,
        schema = schema,
        cem_schema = cemSchema,
        vocab_schema = vocabularySchema,
        summary_table = summaryTable,
        target_cohort_ids = targetCohortIds,
        outcome_cohort_ids = outcomeCohortIds,
      )

      mappedControls <- rbind(mappedControls, parentLevelEvidenceConcepts)
    }
  }

  return(mappedControls)
}