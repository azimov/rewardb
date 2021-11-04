forestPlotUi <- function(id) {
  shiny::tagList(shinycssloaders::withSpinner(plotly::plotlyOutput(NS(id, "forestPlot"), height = 500)),
                 shiny::hr(),
                 shiny::fluidRow(shinydashboard::box(strong("Figure 1."),
                                                     paste("Forest plot of effect estimates from each database"),
                                                     shiny::br(),
                                                     shiny::downloadButton(shiny::NS(id, "downloadForestPlot"), "Save Plot"),
                                                     width = 6),
                                 shinydashboard::box(shinyWidgets::pickerInput(NS(id, "forestPlotCalibrated"),
                                                                        "Display:",
                                                                        choices = list(
                                                                          "Uncalibrated results" = 0,
                                                                          "Calibrated Results" = 1
                                                                        ),
                                                                        selected = c(0, 1),
                                                                        options = shinyWidgets::pickerOptions(
                                                                          actionsBox = TRUE,
                                                                          noneSelectedText = ""
                                                                        ),
                                                                        multiple = TRUE),
                                                     width = 6)))
}

forestPlotServer <- function(id, model, selectedExposureOutcome) {
  server <- shiny::moduleServer(id, function(input, output, session) {
    forestPlotTable <- shiny::reactive({
      s <- selectedExposureOutcome()
      exposureId <- s$TARGET_COHORT_ID
      outcomeId <- s$OUTCOME_COHORT_ID
      calibrationType <- s$calibrationType
      if (length(outcomeId) & length(exposureId)) {
        shiny::updateTabsetPanel(session, "mainPanel", "Detail")
        calibOpts <- if (length(input$forestPlotCalibrated)) input$forestPlotCalibrated else c(0, 1)
        res <- model$getForestPlotTable(exposureId, outcomeId, calibOpts, calibrationType = calibrationType, sourceIds = s$usedDataSources)
        return(res)
      }
      return(data.frame())
    })

    output$forestPlot <- plotly::renderPlotly({
      df <- forestPlotTable()
      if (nrow(df) > 0) {
        return(plotly::ggplotly(forestPlot(df)))
      }
    })

    output$downloadForestPlot <- shiny::downloadHandler(filename = function() {
      s <- selectedExposureOutcome()
      treatment <- s$TARGET_COHORT_ID
      outcome <- s$OUTCOME_COHORT_ID
      paste0(model$schemaName, '-forest-plot-', treatment, "-", outcome, '.png')
    }, content = function(file) {
      df <- forestPlotTable()
      ggplot2::ggsave(file, plot = rewardb::forestPlot(df), device = "png")
    }
    )
  })
  return(server)
}