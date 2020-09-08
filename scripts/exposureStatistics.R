getTotalExposures <- function(
  connection,
  schema = "Scratch.jgilber2",
  mdcdTable = "homer_cohort_mdcd_v1153_dev",
  mdcrTable = "homer_cohort_mdcr_v1152_dev",
  ccaeTable = "homer_cohort_ccae_v1151_dev",
  optumTable = "homer_cohort_optum_v1156_dev"
) {
  sql <- "
  SELECT
    cohort_definition_id/1000 AS ingredient_concept_id,
    short_name,
    mdcd_count, optum_count, mdcr_count, ccae_count,
    mdcd_count + optum_count + mdcr_count + ccae_count AS total_count
  FROM (
    SELECT
      cd.cohort_definition_id,
      cd.short_name,
      CASE
        WHEN mdcd.exp_count IS NULL THEN 0
        ELSE mdcd.exp_count
      END as mdcd_count,
      CASE
        WHEN mdcr.exp_count IS NULL THEN 0
        ELSE mdcr.exp_count
      END as mdcr_count,
      CASE
        WHEN optum.exp_count IS NULL THEN 0
        ELSE optum.exp_count
      END as optum_count,
      CASE
        WHEN ccae.exp_count IS NULL THEN 0
        ELSE ccae.exp_count
      END as ccae_count
    FROM @schema.cohort_definition cd
    
    LEFT JOIN (
        SELECT
        cohort_definition_id,
        count(*) as exp_count
        FROM @schema.@mdcd_table
        GROUP BY cohort_definition_id
    ) mdcd ON mdcd.cohort_definition_id = cd.cohort_definition_id
  
    LEFT JOIN (
        SELECT
        cohort_definition_id,
        count(*) as exp_count
        FROM @schema.@mdcr_table
        GROUP BY cohort_definition_id
    ) mdcr ON mdcr.cohort_definition_id = cd.cohort_definition_id
  
    LEFT JOIN (
        SELECT
        cohort_definition_id,
        count(*) as exp_count
        FROM @schema.@optum_table
        GROUP BY cohort_definition_id
    ) optum ON optum.cohort_definition_id = cd.cohort_definition_id
  
    LEFT JOIN (
        SELECT
        cohort_definition_id,
        count(*) as exp_count
        FROM @schema.@ccae_table
        GROUP BY cohort_definition_id
    ) ccae ON ccae.cohort_definition_id = cd.cohort_definition_id
  ) AS summary
  ORDER BY total_count DESC
  "

  results <- DatabaseConnector::renderTranslateQuerySql(
    connection = connection,
    sql = sql,
    schema = schema,
    optum_table = optumTable,
    mdcd_table = mdcdTable,
    mdcr_table = mdcrTable,
    ccae_table = ccaeTable
  )
}


config <- yaml::read_yaml("config/global-cfg.yml")

connection <- DatabaseConnector::connect(config$cdmDataSource)

results <- getTotalExposures(connection)