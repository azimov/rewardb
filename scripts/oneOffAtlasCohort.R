
atlasId <- NULL
zipFile <- paste(atlasId, "reward-reference.zip")
config <- loadGlobalConfig("config/global-cfg.yml")
connection <- DatabaseConnector::connect(connectionDetails = config$connectionDetails)
insertAtlasCohortRef(connection, config, atlasId)
exportAtlasCohortRef(config, atlasId, zipFile)


configId <- paste("atlasRun-", atlasId)
cdmConfigPaths <- c("config/cdm/ccae.yml", "config/cdm/optum.yml", "config/cdm/mcdc.yml", "config/cdm/mcdr.yml", "config/cdm/pharmetrics.yml")

for (cdmConfigPath in cdmConfigPaths) {
  zipFiles <- sccOneOffAtlasCohort(cdmConfig, zipFilePath, configId)
  for (zipFile in zipFiles) {
    importResultsZip(zipFile, unzipPath = paste(configId, "import_folder") )
  }
}
