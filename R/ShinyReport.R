
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

  message("Init report dashboard")

  exposureCohorts <-  model$getExposureCohorts()
  outcomeCohorts <- model$getOutcomeCohorts()

  message("Loaded startup cache")

  updateSelectizeInput(session, "outcomeCohorts", choices = outcomeCohorts$cohortDefinitionName, server = TRUE)
  updateSelectizeInput(session, "targetCohorts", choices = exposureCohorts$cohortDefinitionName, server = TRUE)

  message("Loaded  inputs")

  getExposureCohort <- reactive({
    exposureCohorts[exposureCohorts$cohortDefinitionName %in% input$targetCohorts, ]
  })

  getOutcomeCohort <- reactive({
    outcomeCohorts[outcomeCohorts$cohortDefinitionName %in% input$outcomeCohorts, ]
  })

  observeEvent(input$selectCohorts, {
    output$selectedCohorts <- renderText("selected")
  })

  selectedExposureOutcome <- reactive({
    exposureCohort <- getExposureCohort()
    outcomeCohort <- getOutcomeCohort()
    if (is.null(exposureCohort) || is.null(outcomeCohort)) {
      return(NULL)
    }
    selected <- list(
      TARGET_COHORT_ID = exposureCohort$cohortDefinitionId,
      TARGET_COHORT_NAME = exposureCohort$cohortDefinitionName,
      OUTCOME_COHORT_ID = outcomeCohort$cohortDefinitionId,
      OUTCOME_COHORT_NAME = outcomeCohort$cohortDefinitionName
    )

    return(selected)
  })

  output$selectedExposureId <- renderText({
    s <- selectedExposureOutcome()
     if (is.null(s)) {
      return("")
    }
    return(s$TARGET_COHORT_ID)
  })

  output$selectedOutcomeId <- renderText({
    s <- selectedExposureOutcome()
     if (is.null(s)) {
      return("")
    }
    return(s$OUTCOME_COHORT_ID)
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