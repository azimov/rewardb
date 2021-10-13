library(rewardb)

.requiredVars <- c("globalConfigPath", "analysisId")
.setVars <- sapply(.requiredVars, exists)
if (!any(.setVars)) {
  errorMesage <- paste("\nRequired variable:", names(.setVars)[!.unsetVars], "is not set.")
  stop(errorMesage)
}

runPreComputeNullDistributions(globalConfigPath)