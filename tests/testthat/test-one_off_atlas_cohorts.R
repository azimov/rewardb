fullDbSetup()
createOutcomeCohorts(connection, cdmConfig)
createCohorts(connection, cdmConfig)

test_that("Full data generation and export", {
  atlasId <- c(12047)
  configId <- "atlasRun-exposures"
  resultsFiles <- sccOneOffAtlasCohort(cdmConfigPath, configId, atlasIds = atlasId)

  configId <- "atlasRun-exposures"
  atlasExposureId <- c(19321)
  resultsFiles <- sccOneOffAtlasCohort(cdmConfigPath, configId, atlasIds = atlasExposureId, exposure = TRUE)

})