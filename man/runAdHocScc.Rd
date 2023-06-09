% Generated by roxygen2: do not edit by hand
% Please edit documentation in R/ExecuteDataBuild.R
\name{runAdHocScc}
\alias{runAdHocScc}
\title{oneOffSccResults}
\usage{
runAdHocScc(
  cdmConfigPath,
  globalConfig,
  configId,
  outcomeCohortIds = NULL,
  targetCohortIds = NULL,
  logFileName = NULL
)
}
\arguments{
\item{cdmConfigPath}{- path to cdm config loaded with loadCdmConfig function}

\item{configId}{- string id that will be used as a prefix to store results files}

\item{outcomeCohortIds}{- vector of outcome cohort ids or NULL}

\item{targetCohortIds}{- vector of exposure cohort ids or NULL}

\item{logFileName}{logfile used. If null is based on the passed configId}

\item{.generateCohortStats}{- generate time on treatment and time to outcome stats or not}

\item{getDbId}{- assumes CDM version is stored in the cdm_source table}
}
\description{
Partial reward execution with a subset of targets or outcomes. If both are null this will generate SCC results for all
exposure and outcome pairs. This is only really useful if you're adding an cohort after the full result set has been
generated.
}
