library(rewardb)

buildPgDatabase()
atlasCohorts <- c(7542, 7551, 7552, 7553, 7576, 7543, 7545, 7546, 7507, 7547, 7548, 7549, 7550, 7822,
                  7823, 10357, 11073, 2538, 10605, 15078, 10607, 11643, 12047, 14615, 13719, 10977, 19398)

# Build referent db


config <- loadGlobalConfig("config/global-cfg.yml")
connection <- DatabaseConnector::connect(connectionDetails = config$connectionDetails)
# Add atlas cohorts
for (atlasId in atlasCohorts) {
  insertAtlasCohortRef(connection, config, atlasId)
}

customExposureRefs <- list(
  "IL-17 Inhibitors" = 11721
)

for (conceptSetName in names(customExposureRefs)) {
  insertCustomExposureRef(connection, config, customExposureRefs[[ conceptSetName ]], conceptSetName)
}

DatabaseConnector::disconnect(connection)

exportReferenceTables(config)

cdmConfig <- loadCdmConfig("config/cdm/ccae.yml")
importReferenceTables(cdmConfig, "rewardb-references.zip")

generateSccResults("config/cdm/ccae.yml")
importResultsFiles(config$connectionDetails, "rewardb")