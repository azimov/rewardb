devtools::load_all()

cdmConfigPaths <- c(
  "config/cdm/pharmetrics.yml",
  "config/cdm/mcdc.yml",
  "config/cdm/mcdr.yml",
)

getType0OutcomeIds <- function(cdmConfig) {
  conn <- DatabaseConnector::connect(cdmConfig$connectionDetails)
  sql <- "SELECT DISTINCT cohort_definition_id FROM @schema.@outcome_cohort_definition WHERE outcome_type = 0"
  res <- DatabaseConnector::renderTranslateQuerySql(conn, sql, schema = cdmConfig$referenceSchema, outcome_cohort_definition = cdmConfig$tables$outcomeCohortDefinition)
  ids <- res$COHORT_DEFINITION_ID
  DatabaseConnector::disconnect(conn)
  return(ids)
}
.getLogger("type0_outcome_scc.log")
for (cdmConfigPath in cdmConfigPaths) {
  cdmConfig <- loadCdmConfig(cdmConfigPath)
  outcomeIds <- getType0OutcomeIds(cdmConfig)
  oneOffSccResults(cdmConfigPath, paste0("type0_outcome_cohorts", cdmConfig$database), outcomeCohortIds = outcomeIds)
}
