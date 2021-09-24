fullDbSetup()
createCustomDrugEras(cdmConfigPath)
createOutcomeCohorts(connection, cdmConfig)
createCohorts(connection, cdmConfig)

test_that("Full data generation and export", {
  atlasId <- c(12047)
  configId <- "atlasRun-outcomes"

  resultsFiles <- sccAdHocCohorts(cdmConfigPath, configId, atlasIds = atlasId, sourceUrl = config$webApiUrl)
  for (file in Sys.glob(paste0(configId, "/*.csv"))) {
    pgCopy(config$connectionDetails, file, config$rewardbResultsSchema, "scc_result")
  }

  configId <- "atlasRun-exposures"
  atlasExposureId <- c(19321)
  sccAdHocCohorts(cdmConfigPath, configId, atlasIds = atlasExposureId, sourceUrl = config$webApiUrl, exposure = TRUE)

  for (file in Sys.glob(paste0(configId, "/*.csv"))) {
    pgCopy(config$connectionDetails, file, config$rewardbResultsSchema, "scc_result")
  }

})