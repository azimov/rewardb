forestPlotUi <- function(id) {
  tagList(
    withSpinner(plotly::plotlyOutput(NS(id, "forestPlot"), height = 500)),
    hr(),
    fluidRow(
      box(
        strong("Figure 1."),
        paste("Forest plot of effect estimates from each database"),
        br(),
        downloadButton(NS(id, "downloadForestPlot"), "Save Plot"),
        width = 6
      ),
      box(
        pickerInput(
          NS(id, "forestPlotCalibrated"),
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
          multiple = TRUE
        ),
        width = 6
      )
    )
  )
}

forestPlotServer <- function(id, model, selectedExposureOutcome) {
  server <- moduleServer(id, function(input, output, session) {
    forestPlotTable <- reactive({
      s <- selectedExposureOutcome()
      exposureId <- s$TARGET_COHORT_ID
      outcomeId <- s$OUTCOME_COHORT_ID
      if (length(outcomeId)) {
        updateTabsetPanel(session, "mainPanel", "Detail")
        calibOpts <- if (length(input$forestPlotCalibrated)) input$forestPlotCalibrated else c(0, 1)
        return(model$getForestPlotTable(exposureId, outcomeId, calibOpts))
      }
      return(data.frame())
    })

    output$forestPlot <- plotly::renderPlotly({
      df <- forestPlotTable()
      if (nrow(df)) {
        return(plotly::ggplotly(forestPlot(df)))
      }
    })

    output$downloadForestPlot <- downloadHandler(
      filename = function() {
        s <- selectedExposureOutcome()
        treatment <- s$TARGET_COHORT_ID
        outcome <- s$OUTCOME_COHORT_ID
        paste0(model$schemaName, '-forest-plot-', treatment, "-", outcome, '.png')
      },
      content = function(file) {
        df <- forestPlotTable()
        ggplot2::ggsave(file, plot = rewardb::forestPlot(df), device = "png")
      }
    )
  })
  return(server)
}