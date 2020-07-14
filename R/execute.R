fullExecution <- function(configFilePath = "config/global-cfg.yml") {
  # load config
  config <- yaml::read_yaml(configFilePath)
  connection <- DatabaseConnector::connect(config$cdmDataSource)

  base::writeLines("Creating and populating reference tables...")
  createReferenceTables(connection, config)

  base::writeLines("Adding maintained atlas cohorts from config")
  for (aid in conifg$maintinedAtlasCohortList) {
    removeAtlasCohort(connection, config, aid)
    addAtlasCohort(connection, config, aid)
  }
  base::writeLines("Creating exposure cohorts")
  createCohorts(connection, config)
  base::writeLines("Creating outcome cohorts")
  createOutcomeCohorts(connection, config)
  base::writeLines("Creating custom outcome cohorts")
  addCustomOutcomes(connection, config)
  # generate summary tables
  base::writeLines("Generating cohort summary tables")
  createSummaryTables(connection, config)
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
  addAtlasCohort(connection, config, atlasId)

  # Only adds where null entries are dound in the table so doesn't need an id
  addCustomOutcomes(connection, config)

  # TODO: add summary
  # TODO: run sccs
  for (dataSource in config$dataSources) {
    addCustomOutcome(connection, config, dataSource, atlasId)
  }
  # TODO: add to full results tables
}