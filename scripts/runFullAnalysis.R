devtools::load_all()

atlasCohorts <- c(7542, 7551, 7552, 7553, 7576, 7543, 7545, 7546, 7507, 7547, 7548, 7549, 7550, 7822,
                  7823, 10357, 11073, 2538, 10605, 15078, 10607, 11643, 12047, 14615, 13719, 10977, 19398)

config <- loadGlobalConfig("config/global-cfg.yml")
connection <- DatabaseConnector::connect(connectionDetails = config$connectionDetails)
# Add atlas cohorts
for (atlasId in atlasCohorts) {
  insertAtlasCohortRef(connection, config, atlasId)
}

atlasExposureCohorts <- c(19177, 19178)

for (atlasId in atlasExposureCohorts) {
  insertAtlasCohortRef(connection, config, atlasId, exposure = TRUE)
}


customExposureRefs <- list(
  "IL-17 Inhibitors" = 11721
)

for (conceptSetName in names(customExposureRefs)) {
  insertCustomExposureRef(connection, config, customExposureRefs[[ conceptSetName ]], conceptSetName)
}

DatabaseConnector::disconnect(connection)

exportReferenceTables(config)

cdmConfigPaths <- c(
  "config/cdm/pharmetrics.yml",
  "config/cdm/mcdc.yml",
  "config/cdm/mcdr.yml",
  "config/cdm/ccae.yml",
  "config/cdm/optum.yml"
)

for (cdmConfigPath in cdmConfigPaths) {
  cdmConfig <- loadCdmConfig(cdmConfigPath)
  importReferenceTables(cdmConfig, "rewardb-references.zip")
  zips <- generateSccResults("config/cdm/ccae.yml")
  for (zip in zips) {
    importResultsFiles(config$connectionDetails, "rewardb", zip)
  }
}
