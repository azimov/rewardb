#'@export
fullExecution <- function(
  configFilePath = "config/global-cfg.yml",
  createReferences = TRUE,
  addDefaultAtlasCohorts = TRUE,
  .createExposureCohorts = TRUE,
  .createOutcomeCohorts = TRUE,
  .generateSummaryTables = TRUE
) {
  # load config
  config <- yaml::read_yaml(configFilePath)
  connection <- DatabaseConnector::connect(config$cdmDataSource)

  if (createReferences) {
    base::writeLines("Creating and populating reference tables...")
    createReferenceTables(connection, config)
  }

  if (length(config$maintinedAtlasCohortList) & addDefaultAtlasCohorts) {
    base::writeLines(paste("removing atlas cohort", config$maintinedAtlasCohortList))
    removeAtlasCohort(connection, config, config$maintinedAtlasCohortList)
    base::writeLines("Adding maintained atlas cohorts from config")

    for (aid in config$maintinedAtlasCohortList) {
      base::writeLines(paste("adding cohort", aid))
      insertAtlasCohort(connection, config, aid)
    }
  }
 
  if (.createExposureCohorts) {
    base::writeLines("Creating exposure cohorts")
    createCohorts(connection, config)
  }
  # NOT tested from here
  if (.createOutcomeCohorts) {
    base::writeLines("Creating outcome cohorts")
    createOutcomeCohorts(connection, config)
  }

  # Always runs as this checks for null entries
  base::writeLines("Creating custom outcome cohorts")
  addCustomOutcomes(connection, config)
  # generate summary tables
  if (.generateSummaryTables) {
    base::writeLines("Generating cohort summary tables")
    createSummaryTables(connection, config)
  }

  base::writeLines("Generating fresh scc results tables")
  createResultsTables(connection, config)
  # run SCC
  for (dataSource in config$dataSources) {
    base::writeLines(paste("Running scc on", dataSource$database))
    batchScc(connection, config, dataSource)
  }
  compileResults(connection, config)

  # TODO: run SCCS
}

addAtlasCohort <- function(configFilePath = "config/global-cfg.yml", atlasId, removeExisting = FALSE) {
  config <- yaml::read_yaml(configFilePath)
  connection <- DatabaseConnector::connect(config$cdmDataSource)
  if (removeExisting) {
    removeAtlasCohort(connection, config, atlasId)
  }
  insertAtlasCohort(connection, config, atlasId)

  # Only adds where null entries are dound in the table so doesn't need an id
  addCustomOutcomes(connection, config)

  # TODO: add summary
  # TODO: run sccs
  for (dataSource in config$dataSources) {
    addCustomOutcome(connection, config, dataSource, atlasId)
  }
  # TODO: add to full results tables
}