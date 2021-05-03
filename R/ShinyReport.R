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
  library(dplyr, warn.conflicts = FALSE)
  library(shinymanager)

  secureApplication <- getOption("reward.secure", default = FALSE)

  if (secureApplication) {
    # define some credentials
    credentials <- data.frame(
      user = c("reward_user", "jgilber2"), # mandatory
      password = c("ohda-prod-1", "hangman252"), # mandatory
      admin = c(FALSE, TRUE)
    )

    res_auth <- secure_server(
      check_credentials = check_credentials(credentials)
    )

    output$auth_output <- renderPrint({
      reactiveValuesToList(res_auth)
    })
  }

  message("Init report dashboard")

  exposureCohorts <- model$getExposureCohorts()
  outcomeCohorts <- model$getOutcomeCohorts()
  dataSources <-  model$getDataSourceInfo()

  message("Loaded startup cache")

  updateSelectizeInput(session, "outcomeCohorts", choices = outcomeCohorts$cohortDefinitionName, server = TRUE)
  updateSelectizeInput(session, "targetCohorts", choices = exposureCohorts$cohortDefinitionName, server = TRUE)
  shinyWidgets::updatePickerInput(session, "dataSourcesUsed", choices = dataSources$sourceName, selected = dataSources$sourceName)

  message("Loaded  inputs")

  output$outcomeCohortsTable <- DT::renderDataTable({
    outcomeCohorts
  })

  output$exposureCohortsTable <- DT::renderDataTable({
    exposureCohorts
  })

  output$dataSourcesTable <- DT::renderDataTable({
    model$getDataSourceInfo()
  })

  output$dataQaulityTable <- gt::render_gt({
    exposureOutcomePairs <- data.frame(
      exposureId = c(7869, 7869, 8163, 8163),
      outcomeId = c(311525, 311526, 345050, 345074)
    )

    baseData <- model$getExposureOutcomeDqd(exposureOutcomePairs) %>%
      gt::gt() %>%
      gt::fmt_number(decimals = 2, columns = c(3, 4, 7, 8)) %>%
      gt::tab_options(
        table.font.size = "tiny"
      )

    return(baseData)
  })

  getExposureCohort <- reactive({
    exposureCohorts[exposureCohorts$cohortDefinitionName %in% input$targetCohorts,]
  })

  getOutcomeCohort <- reactive({
    outcomeCohorts[outcomeCohorts$cohortDefinitionName %in% input$outcomeCohorts,]
  })

  output$selectedCohorts <- renderText("not selected")

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
      OUTCOME_COHORT_NAME = outcomeCohort$cohortDefinitionName,
      calibrationType = input$calibrationType,
      usedDataSources = dataSources[dataSources$sourceName %in% input$dataSourcesUsed,]$sourceId
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

  calibrationPlotServer("outcomeCalibrationPlot", model, selectedExposureOutcome, FALSE)
  calibrationPlotServer("exposureCalibrationPlot", model, selectedExposureOutcome, TRUE)

  timeOnTreatmentServer("timeOnTreatment", model, selectedExposureOutcome)
  tabPanelTimeOnTreatment <- tabPanel("Time on treatment", boxPlotModuleUi("timeOnTreatment"))
  shiny::appendTab(inputId = "searchResults", tabPanelTimeOnTreatment)

  timeToOutcomeServer("timeToOutcome", model, selectedExposureOutcome)
  tabPanelTimeToOutcome <- tabPanel("Time to outcome", boxPlotModuleUi("timeToOutcome"))
  shiny::appendTab(inputId = "searchResults", tabPanelTimeToOutcome)

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

  secureApplication <- getOption("reward.secure", default = FALSE)

  if (secureApplication) {
    ui <- shinymanager::secure_app(reportUi)
  } else {
    ui <- reportUi
  }
  shiny::shinyApp(server = reportInstance, ui = ui, onStart = function() {
    shiny::onStop(function() {
      writeLines("Closing connection")
      model$closeConnection()
    })
  })
}