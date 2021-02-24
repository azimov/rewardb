devtools::load_all()
config <- loadGlobalConfig("config/global-cfg.yml")
connection <- DatabaseConnector::connect(connectionDetails = config$connectionDetails)

# Set up a database schema
buildPgDatabase("config/global-cfg.yml")


atlasCohorts <- c(7542, 7551, 7552, 7553, 7576, 7543, 7545, 7546, 7507, 7547, 7548, 7549, 7550, 7822,
                  7823, 10357, 11073, 2538, 10605, 15078, 10607, 11643, 12047, 14615, 13719, 10977, 19398)

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

for (cdmPath in Sys.glob("config/cdm/*.yml")) {
  cdmConfig <- loadCdmConfig(cdmPath)
  registerCdm(connection, config, cdmConfig)
}

DatabaseConnector::disconnect(connection)
