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


forestPlotServer <- function(id, dbConn, queryDb, appContext, selectedExposureOutcome) {
  server <- moduleServer(id, function(input, output, session) {
    forestPlotTable <- reactive({
      s <- selectedExposureOutcome()
      treatment <- s$TARGET_COHORT_ID
      outcome <- s$OUTCOME_COHORT_ID
      if (length(outcome)) {
        updateTabsetPanel(session, "mainPanel", "Detail")
        sql <- readr::read_file(system.file("sql/queries/", "getTargetOutcomeRows.sql", package = "rewardb"))

        calibOpts <- if (length(input$forestPlotCalibrated)) input$forestPlotCalibrated else c(0, 1)

        table <- queryDb(sql, treatment = treatment, outcome = outcome, calibrated = calibOpts)
        calibratedTable <- table[table$CALIBRATED == 1,]
        uncalibratedTable <- table[table$CALIBRATED == 0,]

        if (nrow(calibratedTable) & nrow(uncalibratedTable)) {
          calibratedTable$calibrated <- "Calibrated"
          uncalibratedTable$calibrated <- "Uncalibrated"
          uncalibratedTable$SOURCE_NAME <- paste0(uncalibratedTable$SOURCE_NAME, "\n uncalibrated")
          calibratedTable$SOURCE_NAME <- paste0(calibratedTable$SOURCE_NAME, "\n Calibrated")
        }

        table <- rbind(uncalibratedTable[order(uncalibratedTable$SOURCE_ID, decreasing = TRUE),],
                       calibratedTable[order(calibratedTable$SOURCE_ID, decreasing = TRUE),])
        return(table)
      }
      return(data.frame())
    })

    output$forestPlot <- plotly::renderPlotly({
      df <- forestPlotTable()
      if (nrow(df)) {
        return(plotly::ggplotly(rewardb::forestPlot(df)))
      }
    })

    output$downloadForestPlot <- downloadHandler(
      filename = function() {
        s <- selectedExposureOutcome()
        treatment <- s$TARGET_COHORT_ID
        outcome <- s$OUTCOME_COHORT_ID
        paste0(appContext$short_name, '-forest-plot-', treatment, "-", outcome, '.png')
      },
      content = function(file) {
        df <- forestPlotTable()
        ggplot2::ggsave(file, plot = rewardb::forestPlot(df), device = "png")
      }
    )
  })
  return(server)
}