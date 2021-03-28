devtools::load_all()
atlasIds <- c(7542, 7551, 7552, 7553, 7576, 7543, 7545, 7546, 7507, 7547, 7548, 7549, 7550, 7822,
             7823, 10357, 11073, 2538, 10605, 15078, 10607, 11643, 12047, 14615, 13719, 10977, 19398)

refZipFile <- "atlas-pre-existing-reward-reference.zip"
config <- loadGlobalConfig("config/global-cfg.yml")

configId <- paste("atlasRun-pre-existing")
cdmConfigPaths <-c(
  "config/cdm/mcdc.yml",
  "config/cdm/mcdr.yml",
  "config/cdm/ccae.yml",
  "config/cdm/optum.yml"
)


for (cdmConfigPath in cdmConfigPaths) {
  resultsFiles <- sccAdHocCohorts(cdmConfigPath, configId, atlasIds, sourceUrl = config$webApiUrl)
  # Copy files
  for (table in names(resultsFiles)) {
    for (file in resultsFiles[[table]]) {
      pgCopy(config$connectionDetails, file, config$rewardbResultsSchema, table)
    }
  }
}


## Exposure cohorts
atlasExposureId <- c(19177, 19178)
resultsFiles <- c()
for (cdmConfigPath in cdmConfigPaths) {
  resultsFiles <- sccAdHocCohorts(cdmConfigPath, configId, atlasExposureId, sourceUrl = config$webApiUrl, exposure = TRUE)
  # Copy files
  for (table in names(resultsFiles)) {
    for (file in resultsFiles[[table]]) {
      pgCopy(config$connectionDetails, file, config$rewardbResultsSchema, table)
    }
  }
}
