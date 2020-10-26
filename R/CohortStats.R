
# Compute the average time on treatement for cohort pairs
# TODO: tidy up and optimize the sql to have this pre computed
getAverageTimeOnTreatment <- function (connection, config, targetCohortIds = NULL, outcomeCohortIds = NULL) {
  pathToSqlFile <- system.file("sql/queries", "averageTimeOnTreatment.sql", package = "rewardb")
  sql <- SqlRender::readSql(pathToSqlFile)
  results <- data.frame()

  res <- DatabaseConnector::renderTranslateQuerySql(
    connection,
    sql,
    schema = config$resultSchema,
    subset_targets = ifelse(length(targetCohortIds), 1, 0),
    target_cohort_ids = targetCohortIds,
    outcome_cohort_table = config$tables$outcomeCohort,
    cohort_table = config$tables$cohort,
    outcome_cohort_ids = outcomeCohortIds,
    subset_outcomes = ifelse(length(outcomeCohortIds), 1, 0),
    cdm_schema = config$cdmSchema
  )
  res$source_id <- config$sourceId
  results <- rbind(results, res)

  return(results)
}


getDashboardCohortStatistics <- function(connection, cdmConnection, config, appContext) {
  targetCohortIds <- appContext$targetCohortIds
  outcomeCohortIds <- append(appContext$outcomeCohortIds, appContext$custom_outcome_cohort_ids)

  timeOnTreatment <- getAverageTimeOnTreatment(cdmConnection, config, targetCohortIds = targetCohortIds, outcomeCohortIds = outcomeCohortIds)

  tableName = paste(appContext$short_name, "time_on_treatment", sep = ".")
  readr::write_excel_csv(timeOnTreatment, paste0("data/", tableName, ".csv"))
  DatabaseConnector::insertTable(connection, tableName, timeOnTreatment)
}