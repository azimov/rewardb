devtools::load_all()
#atlasIds <- c(7542, 7551, 7552, 7553, 7576, 7543, 7545, 7546, 7507, 7547, 7548, 7549, 7550, 7822,
#7823, 10357, 11073, 2538, 10605, 15078, 10607, 11643, 12047, 14615, 13719, 10977, 19398, 20024, 20042, 20001, 17619, 10977, 20655, 19511, 19514, 19550, 19802, 19804, 20004, 20364, 20421)

atlasIds <- c(382,381)
refZipFile <- "reward-reference.zip"
config <- loadGlobalConfig("config/global-cfg.yml")

ROhdsiWebApi::authorizeWebApi(config$webApiUrl, "windows")

addCohorts <- function() {
  connection <- DatabaseConnector::connect(connectionDetails = config$connectionDetails)
  on.exit(DatabaseConnector::disconnect(connection))
  # Add atlas cohorts
  for (atlasId in atlasIds) {
    insertAtlasCohortRef(connection, config, atlasId)
  }

}

addCohorts()
exportReferenceTables(config, export = "reference_files", exportZipFile = refZipFile)


configId <- paste("atlasRun-lung-cancer-cohorts")
cdmConfigPaths <-c(
  "config/cdm/mdcd.yml",
  "config/cdm/mdcr.yml",
  "config/cdm/ccae.yml",
  "config/cdm/optum.yml",
  "config/cdm/jmdc.yml"
)


for (cdmConfigPath in cdmConfigPaths) {
  resultsFiles <- sccAdHocCohorts(cdmConfigPath, configId, atlasIds, config$webApiUrl, referenceZipFile = refZipFile)
  # Copy files
}

## Exposure cohorts
atlasExposureId <- c()
resultsFilesMas <- list()
for (cdmConfigPath in cdmConfigPaths) {
  resultsFilesMas[cdmConfigPath] <- sccAdHocCohorts(cdmConfigPath, configId, atlasExposureId, exposure = TRUE)
}
# Copy files

for (file in Sys.glob(file.path(configId, "*time_on_treatment*"))) {
  pgCopy(config$connectionDetails, file, config$rewardbResultsSchema, "time_on_treatment")
}

for (file in Sys.glob(file.path(configId, "*aid-[1,2].csv"))) {
    pgCopy(config$connectionDetails, file, config$rewardbResultsSchema, "scc_result")
}
