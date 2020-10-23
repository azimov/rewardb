
# Compute the average time on treatement for cohort pairs
# TODO: tidy up and optimize the sql to have this pre computed
getAverageTimeOnTreatment <- function (connection, config, targetCohortIds = NULL, outcomeCohortIds = NULL) {
  pathToSqlFile <- system.file("sql/queries", "averageTimeOnTreatment.sql", package = "rewardb")
  sql <- SqlRender::readSql(pathToSqlFile)
  results <- data.frame()
  for (dataSource in config$dataSources) {
      res <- DatabaseConnector::renderTranslateQuerySql(
        connection,
        sql,
        schema = config$cdmDatabase$schema,
        subset_targets = ifelse(length(targetCohortIds), 1, 0),
        cohort_ids = targetCohortIds,
        outcome_cohort_table = dataSource$outcomeCohortTable,
        cohort_table = dataSource$cohortTable,
        outcome_cohort_ids = outcomeCohortIds,
        subset_outcomes = ifelse(length(outcomeCohortIds), 1, 0),
        cdm_schema = dataSource$cdmDatabaseSchema
      )
      res$source_id <- dataSource$sourceId
      res$source <- dataSource$database
      results <- rbind(results, res)
  }

  return(results)
}


getDashboardCohortStatistics <- function(connection, cdmConnection, config, appContext) {
  targetCohortIds <- appContext$targetCohortIds
  outcomeCohortIds <- append(appContext$outcomeCohortIds, appContext$custom_outcome_cohort_ids)

  timeOnTreatment <- getAverageTimeOnTreatment(cdmConnection, config, targetCohortIds = targetCohortIds, outcomeCohortIds = outcomeCohortIds)

  tableName = paste(appContext$short_name, "time_on_treatment_stats", sep = ".")
  readr::write_excel_csv(timeOnTreatment, paste0(tableName, ".csv"))
  DatabaseConnector::insertTable(connection, tableName, timeOnTreatment)
}