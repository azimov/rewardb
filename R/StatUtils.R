
# Compute the average time on treatement for cohort pairs
# TODO: tidy up and optimize the sql to have this pre computed
averageTimeOnTreatment <- function (connection, config, targetCohortIds, outcomeCohortIds) {
  pathToSqlFile <- system.file("sql/queries", "timeOnTreatment.sql", package = "rewardb")
  sql <- SqlRender::readSql(pathToSqlFile)
  results <- data.frame()
  for (dataSource in config$dataSources) {
    for (cohortId in targetCohortIds) {
      for (outcomeCohortId in outcomeCohortIds) {
        res <- DatabaseConnector::renderTranslateQuerySql(
          connection,
          sql,
          schema = config$cdmDatabase$schema,
          cohort_id = cohortId,
          outcome_cohort_table = dataSource$outcomeCohortTable,
          cohort_table = dataSource$cohortTable,
          outcome_cohort_id = outcomeCohortId,
          cdm_schema = dataSource$cdmDatabaseSchema
        )
        res$source_id <- dataSource$sourceId
        res$source <- dataSource$database
        results <- rbind(results, res)
      }
    }
  }

  return(results)
}
