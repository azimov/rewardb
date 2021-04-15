devtools::load_all()
#atlasIds <- c(7542, 7551, 7552, 7553, 7576, 7543, 7545, 7546, 7507, 7547, 7548, 7549, 7550, 7822,
             #7823, 10357, 11073, 2538, 10605, 15078, 10607, 11643, 12047, 14615, 13719, 10977, 19398)

atlasIds <- c(20024, 20042, 20001, 17619, 10977, 20655, 19511, 19514, 19550, 19802, 19804, 20004, 20364, 20421)

refZipFile <- "ns-reward-reference.zip"
config <- loadGlobalConfig("config/global-cfg.yml")

configId <- paste("atlasRun-pre-existing")
cdmConfigPaths <-c(
  "config/cdm/mdcd.yml",
  "config/cdm/mdcr.yml",
  "config/cdm/ccae.yml",
  "config/cdm/optum.yml",
  "config/cdm/jdmc.yml",
)


for (cdmConfigPath in cdmConfigPaths) {
  resultsFiles <- sccAdHocCohorts(cdmConfigPath, configId, atlasIds)
  # Copy files
  for (table in names(resultsFiles)) {
    for (file in resultsFiles[[table]]) {
      pgCopy(config$connectionDetails, file, config$rewardbResultsSchema, table)
    }
  }
}


## Exposure cohorts
atlasExposureId <- c()
resultsFilesMas <- list()
for (cdmConfigPath in cdmConfigPaths) {
  resultsFilesMas[cdmConfigPath] <- sccAdHocCohorts(cdmConfigPath, configId, atlasExposureId, exposure = TRUE)
}
# Copy files

for (resultsFiles in Sys.glob("atlasRun-pre-existing/*time_on_treatment*")) {
  #for (table in names(resultsFiles)) {
    for (file in resultsFiles[[table]]) {
      pgCopy(config$connectionDetails, file, config$rewardbResultsSchema, "time_on_treatment")
    }
  #}
}