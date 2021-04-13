remotes::install_github("OHDSI/SelfControlledCohort", dependencies = TRUE)

cdmSets <- list(
  list(
    sourceId = 10,
    cdm = "CDM_Optum_Extended_SES_v1156",
    cohortTable = "homer_cohort_optum_v1156_dev",
    outcomeCohortTable = "homer_outcome_cohort_optum_v1156_dev",
    cdmDatabaseSchema = "CDM_Optum_Extended_SES_v1156.dbo"
  ),
  list(
    sourceId = 11,
    cdm = "CDM_IBM_ccae_v1151",
    cohortTable = "homer_cohort_ccae_v1151_dev",
    outcomeCohortTable = "homer_outcome_cohort_ccae_v1151_dev",
    cdmDatabaseSchema = "CDM_IBM_ccae_v1151.dbo"
  ),
  list(
    sourceId = 12,
    cdm = "CDM_IBM_MDCD_v1153",
    cohortTable = "homer_cohort_mdcd_v1153_dev",
    outcomeCohortTable = "homer_outcome_cohort_mdcd_v1153_dev",
    cdmDatabaseSchema = "CDM_IBM_MDCD_v1153.dbo"
  ),
  list(
    sourceId = 13,
    cdm = "CDM_IBM_MDCR_v1152",
    cohortTable = "homer_cohort_mdcr_v1152_dev",
    outcomeCohortTable = "homer_outcome_cohort_mdcr_v1152_dev",
    cdmDatabaseSchema = "CDM_IBM_MDCR_v1152.dbo"
  )
)

connectionDetails <- DatabaseConnector::createConnectionDetails(dbms = 'pdw',
                                                                server = 'JRDUSAPSCTL01',
                                                                port = 17001)


outcomeCohortIds = c(7545)
targetCohorts = data.frame(
  cohortDefinitionId = c(1353766000, 742185000, 731533000, 719311000, 1344965000, 705944000, 19043959000,
                         735850000, 1714319000, 718583000, 1398937000, 1110727000, 777221000, 1350489000, 781182000),
  cohortName = c("propranolol", "atomoxetine", "dexmethylphenidate", "dextroamphetamine", "guanfacine", "methylphenidate", "acamprosate",
                 "disulfiram", "naltrexone", "nicotine", "clonidine", "cyproheptadine", "hydroxyzine", "prazosin", "ramelteon")
)

resultsSchema <- "scratch.jgilber2"


res <- list()
combined <- data.frame()
for (cdm in cdmSets) {

  result <- SelfControlledCohort::runSelfControlledCohort(
    connectionDetails = connectionDetails,
    outcomeTable = cdm$outcomeCohortTable,
    exposureTable = cdm$cohortTable,
    exposureDatabaseSchema = resultsSchema,
    outcomeDatabaseSchema = resultsSchema,
    cdmDatabaseSchema = cdm$cdmDatabaseSchema,
    outcomeIds = outcomeCohortIds,
    exposureIds = targetCohorts$cohortDefinitionId,
    firstExposureOnly = TRUE,
    firstOutcomeOnly = TRUE,
    minAge = "",
    maxAge = "",
    studyStartDate = "",
    studyEndDate = "",
    addLengthOfExposureExposed = TRUE,
    riskWindowStartExposed = 1,
    riskWindowEndExposed = 1,
    addLengthOfExposureUnexposed = TRUE,
    riskWindowEndUnexposed = -1,
    riskWindowStartUnexposed = -1,
    hasFullTimeAtRisk = TRUE,
    washoutPeriod = 0,
    followupPeriod = 0,
    computeTarDistribution = TRUE
  )
  result$estimates$sourceId <- cdm$sourceId
  result$estimates$nTotal <- result$estimates$numOutcomesExposed + result$estimates$numOutcomesUnexposed
  combined <- rbind(combined, result$estimates)
}

metaMean <- function(grpData) {
  merged <- meta::metamean(grpData$nTotal, grpData$meanTxTime, grpData$sdTxTime)

  data.frame(metaMeanTxTimeFixed = merged$mean[[1]],
             metaSdTxTimeFixed = merged$sd[[1]],
             metaMeanTxTimeRandom = merged$mean[[2]],
             metaSdTxTimeRandom = merged$sd[[2]])
}

library(dplyr)
# Calculate weighted mean and standard deviations for meta-analysis
combined$nTotal <- combined$numOutcomesExposed + combined$numOutcomesUnexposed
treatmentTimes <- combined %>% group_by(exposureId, outcomeId) %>% group_modify(~metaMean(.x)) %>%
  select(exposureId, metaMeanTxTimeRandom, metaSdTxTimeRandom) %>%
  inner_join(targetCohorts, by = c("exposureId" = "cohortDefinitionId"))
