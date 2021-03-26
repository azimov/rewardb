fullDbSetup()
createOutcomeCohorts(connection, cdmConfig)
createCohorts(connection, cdmConfig)

test_that("Full data generation and export", {
  atlasId <- c(12047)
  configId <- "atlasRun-exposures"

  resultsFiles <- sccAdHocCohorts(cdmConfigPath, configId, atlasIds = atlasId, sourceUrl = config$webApiUrl)

  for (table in names(resultsFiles)) {
    for (file in resultsFiles[[table]]) {
      pgCopy(config$connectionDetails, file, config$rewardbResultsSchema, table)
    }
  }

  configId <- "atlasRun-exposures"
  atlasExposureId <- c(19321)
  resultsFiles <- sccAdHocCohorts(cdmConfigPath, configId, atlasIds = atlasExposureId, sourceUrl = config$webApiUrl, exposure = TRUE)

  for (table in names(resultsFiles)) {
    for (file in resultsFiles[[table]]) {
      pgCopy(config$connectionDetails, file, config$rewardbResultsSchema, table)
    }
  }

})