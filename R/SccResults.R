#' Peform SCC from self controlled cohort package with rewardbs settings
runScc <- function(postProcessFunction,
                   postProcessArgs,
                   cdmConfig,
                   globalConfig,
                   analysisId,
                   analysisSettings,
                   exposureIds = NULL,
                   outcomeIds = NULL,
                   cores = parallel::detectCores() - 1) {
  ParallelLogger::logInfo(paste("Starting SCC analysis on", cdmConfig$database))

  if (is.null(exposureIds)) {
    exposureIds <- ""
  }
  if (is.null(outcomeIds)) {
    outcomeIds <- ""
  }
  opts <- list(connectionDetails = cdmConfig$connectionDetails,
               cdmDatabaseSchema = cdmConfig$cdmSchema,
               cdmVersion = 5,
               exposureIds = exposureIds,
               outcomeIds = outcomeIds,
               exposureDatabaseSchema = cdmConfig$resultSchema,
               exposureTable = cdmConfig$tables$cohort,
               outcomeDatabaseSchema = cdmConfig$resultSchema,
               outcomeTable = cdmConfig$tables$outcomeCohort,
               computeThreads = cores,
               postProcessFunction = postProcessFunction,
               postProcessArgs = postProcessArgs,
               computeTarDistribution = TRUE)
  args <- c(analysisSettings, opts)
  do.call(SelfControlledCohort::runSelfControlledCohort, args)
  ParallelLogger::logInfo(paste("Completed SCC for", cdmConfig$database))
}