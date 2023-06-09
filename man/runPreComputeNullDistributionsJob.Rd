% Generated by roxygen2: do not edit by hand
% Please edit documentation in R/PreComputeNulls.R
\name{runPreComputeNullDistributionsJob}
\alias{runPreComputeNullDistributionsJob}
\title{Run pre compute null distributions Rstudio Job}
\usage{
runPreComputeNullDistributionsJob(
  globalConfigPath,
  analysisId = 1,
  sourceIds = NULL,
  nThreads = 10,
  getCemMappings = TRUE,
  minCohortSize = 5,
  workingDir = getwd()
)
}
\arguments{
\item{globalConfigPath}{config path}

\item{analysisId}{analysis setting to use}

\item{sourceIds}{data sources}

\item{nThreads}{thread count}

\item{getCemMappings}{get mappings from cem}

\item{minCohortSize}{Compute outcome null distributions}

\item{workingDir}{Charachter working directory (default is currentWd)}
}
\description{
Run task to pre-compute null distributions for all outcomes and exposures as RStudio Job
This process requires a CemConnector connection, this may be slow if using cem.ohdsi.org
}
