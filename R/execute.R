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
      base::writeLines(paste("adding cohort reference", aid))
      insertAtlasCohortRef(connection, config, aid)
    }
  }
 
  if (.createExposureCohorts) {
    base::writeLines("Creating exposure cohorts")
    createCohorts(connection, config)
  }
  # NOT tested from here
  if (.createOutcomeCohorts) {
    for (aid in config$maintinedAtlasCohortList) {
      base::writeLines(paste("Generating custom outcome cohort", aid))
      addAtlasOutcomeCohort(connection, config, aid)
    }

    base::writeLines("Creating outcome cohorts")
    createOutcomeCohorts(connection, config)
  }
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
  insertAtlasCohortRef(connection, config, atlasId)

  # Removes then adds cohort with atlas generated sql
  addAtlasOutcomeCohort(connection, config, atlasId)

  # Adds new cohort to summary table
  addOutcomeSummary(connection, config, atlasId)

  for (dataSource in config$dataSources) {
    generateCustomOutcomeResult(connection, config, dataSource, atlasId)
  }
  compileCustomResults(conection, config, atlasId)
}