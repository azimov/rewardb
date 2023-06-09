% Generated by roxygen2: do not edit by hand
% Please edit documentation in R/Calibration.R
\name{getExposureControls}
\alias{getExposureControls}
\title{Get exposure controls}
\usage{
getExposureControls(appContext, connection)
}
\arguments{
\item{appContext}{rewardb app context}

\item{connection}{DatabaseConnector connection to postgres rewardb instance}

\item{minCohortSize}{smaller cohorts are not generally used for calibration as rr values tend to be extremes}
}
\value{
data.frame of negative control exposures for specified outcomes
}
\description{
Returns a set of specified negative controls for input outcome cohorts
}
