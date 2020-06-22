#' Launch the REWARD-B Shiny app
#'
#' @param configPath path to configuration file
#'
#' @details
#' Launches a Shiny app for a given configuration file
#'
#' @export
launchDashboard <- function (configPath) {
  .GlobalEnv$appContext <- rewardb::loadAppContext(configPath)
  on.exit(rm(appContext, envir = .GlobalEnv))
  appDir <- system.file("dashboard", package = "rewardb")
  shiny::runApp(appDir)
}