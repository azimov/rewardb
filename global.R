library(shinyEventLogger)
set_logging()
set_logging_session()
log_event("Loading data")
source("readConfig.R")

appContext <- loadAppContext("config.covid-resp.yml")
dbConn <- DatabaseConnector::connect(connectionDetails = appContext$connectionDetails)

# CONST Exact strings used in SQL query
scBenefitRisk <- c("none", "one", "most", "all")

niceColumnName <- list(
  "SOURCE_NAME" = "Database",
  "RR" = "Relative Risk",
  "C_AT_RISK" = "N Unexposed",
  "T_AT_RISK" = "N Exposed",
  "C_PT" = "Unexposed time (years)",
  "T_PT" = "Exposed time (years)",
  "C_CASES" = "Unexposed cases",
  "T_CASES" = "Exposed cases",
  "LB_95" = "CI95LB",
  "UB_95" = "CI95UB",
  "P_VALUE" = "P",
  "I2" = "I-square"
)

niceColumnNameInv <- list()

for (n in names(niceColumnName)){
  niceColumnNameInv[niceColumnName[[n]]] <- n
}