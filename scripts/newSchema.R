library(rewardb)
configPath <- "config/global-cfg.yml"
## Test first on local postgres instance with this:
#configPath <- system.file("tests", "test.cfg.yml", package = "rewardb")
config <- loadGlobalConfiguration(configPath)
connection <- DatabaseConnector::connect(connectionDetails = config$connectionDetails)

# Set up a database schema
buildPgDatabase(configPath)

ROhdsiWebApi::authorizeWebApi(config$webApiUrl, "windows", "jgilber2", keyring::key_get("jnj", "jgilber2"))
cohortDefinitions <- ROhdsiWebApi::getDefinitionsMetadata(config$webApiUrl, "cohort")
# Find all cohorts in atlas instance with the tag for REWARD
rewardTag <- "\\[REWARD\\]"

outcomeDefinitions <- cohortDefinitions %>% dplyr::filter(grepl(rewardTag, name))

# Add atlas cohorts
for (atlasId in outcomeDefinitions$id) {
  insertAtlasCohortRef(connection, config, atlasId)
}

rewardExposuresTag <- "\\[REWARD EXPOSURE\\]"
atlasExposureCohorts <- cohortDefinitions %>% dplyr::filter(grepl(rewardExposuresTag, name))
for (atlasId in atlasExposureCohorts$id) {
  insertAtlasCohortRef(connection, config, atlasId, exposure = TRUE)
}

rewardExposureClassTag <- "\\[REWARD EXPOSURE CLASS\\]"
ROhdsiWebApi::authorizeWebApi(config$webApiUrl, "windows", "jgilber2", keyring::key_get("jnj", "jgilber2"))
conceptSetDefinitions <- ROhdsiWebApi::getDefinitionsMetadata(config$webApiUrl, "conceptSet") %>% dplyr::filter(grepl(rewardExposureClassTag, name))

for (i in 1:nrow(conceptSetDefinitions)) {
  insertCustomExposureRef(connection, config, conceptSetDefinitions[i,]$id, conceptSetDefinitions[i,]$name)
}

ROhdsiWebApi::authorizeWebApi(config$webApiUrl, "windows", "jgilber2", keyring::key_get("jnj", "jgilber2"))
addPhenotypeLibrary(connection, config)


for (cdmPath in Sys.glob("config/cdm/*.yml")) {
  cdmConfig <- loadCdmConfiguration(cdmPath)
  registerCdm(connection, config, cdmConfig)
}

DatabaseConnector::disconnect(connection)