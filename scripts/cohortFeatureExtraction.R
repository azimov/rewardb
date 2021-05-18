devtools::load_all()
library(gt)
library(dplyr)


config <- loadGlobalConfig("config/global-cfg.yml")

cdmConfigPaths <- c(
  "config/cdm/jmdc.yml"
)

exposureCohortIds <- c(4618)
outcomeCohortIds <- c(344400)

jsonFile <- system.file("settings/default.json", package = "rewardb")
sccAnalysisSettings <- RJSONIO::fromJSON(jsonFile)[[1]]

tableSpec <- FeatureExtraction::getDefaultTable1Specifications()[1:4,]

tables <- list()

covariateSettings <- FeatureExtraction::createCovariateSettings(useDemographicsGender = TRUE,
                                                                useDemographicsAge = TRUE,
                                                                useDemographicsAgeGroup = TRUE,
                                                                useDemographicsRace = TRUE,
                                                                useDemographicsEthnicity = TRUE,
                                                                useDemographicsIndexYear = TRUE)


ageResults <- data.frame()

for (cdmConfigPath in cdmConfigPaths) {
  cdmConfig <- loadCdmConfig(cdmConfigPath)
  covariateDataList <- getExposedOutcomeFetaures(cdmConfig, analysisOptions = sccAnalysisSettings$options, exposureIds = exposureCohortIds, outcomeIds = outcomeCohortIds, covariateSettings = covariateSettings)
  tables[[cdmConfig$database]] <- list()
  
  for (cohort in names(covariateDataList)) {
    dir.create(file.path("results", cdmConfig$database))
    dataF <- covariateDataList[[cohort]]
    aggregatedcovariateData <- aggregateCovariates(dataF)
    # TODO filter as andromeda object!
    df <- data.frame(aggregatedcovariateData$covariatesContinuous)
    df <- df[df$covariateId == 1002, ] %>% select(countValue, minValue, maxValue, averageValue, standardDeviation, medianValue, p10Value, p25Value, p75Value, p90Value)
    df$cohortPair <- cohort
    df$sourceName <- cdmConfig$name
    ageResults <- rbind(ageResults, df)

    tbl <- FeatureExtraction::createTable1(aggregatedcovariateData, specifications = tableSpec, output = "one column") %>% gt() %>% tab_header(paste(cdmConfig$name, "-", cohort))
    tables[[cdmConfig$database]][cohort] <- tbl
    
    # gtsave(tbl, file.path("results", cdmConfig$database, paste0(cohort,"-table-.html")))
    FeatureExtraction::saveCovariateData(dataF, file.path("results", cdmConfig$database, paste0(cohort, "-feature_extraction.dt")))
  }
}

cohortIdMap <- data.frame(
  cohortPair = c("4618-344400"),
  cohortName = c("Parkinson's Disease [NS PL Definition]")
)

ageResults %>% inner_join(cohortIdMap, by="cohortPair") %>%
  select(cohortName, sourceName, countValue, minValue, maxValue, averageValue, standardDeviation, medianValue, p10Value, p25Value, p75Value, p90Value) %>%
  gt(groupname_col = "sourceName") %>%
  fmt_number(6:7, decimals = 2) %>%
  cols_label(cohortName = "") %>%
  tab_options(row_group.background.color = "lightgrey") %>%
  tab_header("") %>%
  tab_source_note("")