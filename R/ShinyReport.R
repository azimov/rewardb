
#' @title
#' reportInstance
#' @description
#' Requires a server appContext instance to be loaded in environment see scoping of launchDashboard
#' This can be obtained with rewardb::loadAppContext(...)
#' UNDER DEVELOPMENT
#' @param input shiny input object
#' @param output shiny output object
#' @param session shiny session object
reportInstance <- function(input, output, session) {
  library(shiny, warn.conflicts = FALSE)
  library(shinyWidgets, warn.conflicts = FALSE)
  library(scales, warn.conflicts = FALSE)
  library(DT, warn.conflicts = FALSE)
  library(foreach, warn.conflicts = FALSE)
  library(dplyr, warn.conflicts = FALSE)
  ParallelLogger::logDebug("init report dashboard")

  getRequestParams <- reactive({
    parseQueryString(session$clientData$url_search)
  })

  exposureId <- getRequestParams()$exposure_id
  outcomeId <- getRequestParams()$outcome_id

  getExposureCohort <- reactive({
    if (is.null(exposureId)) {
      exposureId <-
    }
    df <- model$getExposureCohort(exposureId)
    return(df)
  })

  getOutcomeCohort <- reactive({
    outcomeId <- getRequestParams()$outcome_id
    if (is.null(outcomeId)) {
      outcomeId <-
    }

    return(model$getOutcomeCohort(outcomeId))
  })

  selectedExposureOutcome <- reactive({
    exposureCohort <- getExposureCohort()
    outcomeCohort <- getOutcomeCohort()
    if (is.null(exposureCohort) || is.null(outcomeCohort)) {
      return(NULL)
    }
    selected <- list(
      TARGET_COHORT_ID = exposureCohort$COHORT_DEFINITION_ID,
      TARGET_COHORT_NAME = exposureCohort$COHORT_DEFINITION_NAME,
      OUTCOME_COHORT_ID = outcomeCohort$COHORT_DEFINITION_ID,
      OUTCOME_COHORT_NAME = outcomeCohort$COHORT_DEFINITION_NAME
    )

    return(selected)
  })

  output$treatmentOutcomeStr <- renderText({
    s <- selectedExposureOutcome()
    ParallelLogger::logDebug("selected")
    if (is.null(s)) {
      return("No cohorts selected")
    }
    return(paste("Exposure of", s$TARGET_COHORT_NAME, "for outcome of", s$OUTCOME_COHORT_NAME))
  })

  ParallelLogger::logDebug("init modules")
  # Create sub modules
  metaAnalysisTableServer("metaTable", model, selectedExposureOutcome)
  forestPlotServer("forestPlot", model, selectedExposureOutcome)
}

#' @title
#' Launch the REWARD Shiny app report
#' @description
#' Launches a Shiny app for a given configuration file
#' @param appConfigPath path to configuration file. This is loaded in to the local environment with the appContext variable
#' @param exposureId exposure cohort id
#' @param outcomeId outcome cohort id
#' @export
launchReport <- function(globalConfigPath) {
  .GlobalEnv$reportAppContext <- loadReportContext(globalConfigPath)
  .GlobalEnv$model <- ReportDbModel(reportAppContext)
  shiny::shinyApp(server = reportInstance, ui = reportUi, onStart = function() {
    shiny::onStop(function() {
      writeLines("Closing connection")
      model$closeConnection()
    })
  })
}