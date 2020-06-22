#' Launch the REWARD-B Shiny app
#'
#' @param configuaration path
#'
#' @details
#' Launches a Shiny app that allows the user to explore a cohort of interest.
#'
#' @export
launchDashboard <- function (configPath) {
  .GlobalEnv$appContext <- rewardb::loadAppContext(configPath)
  on.exit(rm(appContext, envir = .GlobalEnv))
  appDir <- system.file("dashboard", package = "rewardb")
  shiny::runApp(appDir)
}