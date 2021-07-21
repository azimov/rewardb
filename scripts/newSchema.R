library(rewardb)
config <- loadGlobalConfig("config/global-cfg.yml")
connection <- DatabaseConnector::connect(connectionDetails = config$connectionDetails)

# Set up a database schema
buildPgDatabase("config/global-cfg.yml")

atlasCohorts <- c(381, 382, 1257, 1258, 1259, 1260, 1261, 1262, 1263, 1264, 1265, 1267, 1268, 1269, 1270, 1271, 1272,
                  1273, 1274, 1275, 1276, 1277, 1278, 1279, 1280, 1281, 1282, 1283, 1284, 1286, 1287, 1288, 1289, 1290,
                  1291, 1292, 1293, 1294, 1295, 1296)

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
