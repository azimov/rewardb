library(rewardb)

.requiredVars <- c("globalConfigPath", "analysisId", "sourceIds", "nThreads", "getCemMappings", "minCohortSize")
.setVars <- sapply(.requiredVars, exists)
if (!any(.setVars)) {
  errorMesage <- paste("\nRequired variable:", names(.setVars)[!.unsetVars], "is not set.")
  stop(errorMesage)
}

runPreComputeNullDistributions(globalConfigPath,
                               analysisId = analysisId,
                               sourceIds = sourceIds,
                               nThreads = nThreads,
                               getCemMappings = getCemMappings,
                               minCohortSize = minCohortSize)
