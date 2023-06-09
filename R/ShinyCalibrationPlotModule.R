# Title     : Calibration plot module
# Objective : To provide a calibration plot and module
# Created by: jpg
# Created on: 2020-12-14

CONST_CALIBRATION_PLOT_TXT <- "Plot of calibration of effect estimates. Blue dots are negative control effect estimates."

calibrationPlotUi <- function(id, figureTitle = "Figure.", figureText = CONST_CALIBRATION_PLOT_TXT) {
  shiny::tagList(
    shinycssloaders::withSpinner(plotly::plotlyOutput(NS(id, "calibrationPlot"), height = 500)),
    shiny::div(
      shiny::strong(figureTitle),
      paste(figureText),
      shiny::downloadButton(shiny::NS(id, "downloadCalibrationPlot"), "Save")
    ),
    shinycssloaders::withSpinner(DT::dataTableOutput(shiny::NS(id, "nullDistribution")))
  )
}

calibrationPlotServer <- function(id, model, selectedExposureOutcome, useExposureControls) {

  server <- shiny::moduleServer(id, function(input, output, session) {
    ParallelLogger::logInfo("Initialized calibration plot module for: ", model$schemaName)

    dataSources <- model$getDataSources()

    getNegativeControlSubset <- function(treatment, outcome, sourceIds = NULL) {
      if (is.null(sourceIds)) {
        sourceIds <- dataSources$SOURCE_ID[[1]]
      }

      if (useExposureControls) {
        negatives <- model$getExposureControls(outcomeIds = outcome, sourceIds = sourceIds)
      } else {
        negatives <- model$getOutcomeControls(targetIds = treatment, sourceIds = sourceIds)
      }
      return(negatives)
    }

    getNullDist <- reactive({
      s <- selectedExposureOutcome()
      treatment <- s$TARGET_COHORT_ID
      outcome <- s$OUTCOME_COHORT_ID
      nulls <- data.frame()
      if (length(treatment) & length(outcome)) {
        negatives <- getNegativeControlSubset(treatment, outcome, dataSources$SOURCE_ID)
        for (source in unique(negatives$SOURCE_ID)) {
          subset <- negatives[negatives$SOURCE_ID == source,]
          null <- EmpiricalCalibration::fitNull(log(subset$RR), subset$SE_LOG_RR)
          systematicError <- EmpiricalCalibration::computeExpectedAbsoluteSystematicError(null)
          df <- data.frame(
            "SOURCE_ID" = source,
            "n" = nrow(subset),
            "mean" = round(exp(null[["mean"]]), 3),
            "sd" = round(exp(null[["sd"]]), 3),
            "EASE" = round(systematicError, 3)
          )
          nulls <- rbind(nulls, df)
        }
        if (nrow(nulls))
          nulls <- dplyr::inner_join(dataSources, nulls, by = "SOURCE_ID")
      }
      return(nulls)
    })

    output$nullDistribution <- DT::renderDataTable({
      null <- getNullDist()
      output <- DT::datatable(
        null,
        options = list(dom = 't', columnDefs = list(list(visible = FALSE, targets = c(0)))),
        rownames = FALSE,
        selection = "single",
        colnames = c("Source", "N controls", "Mean", "Stdev", "EASE"),
        caption = "Table: null distribution mean, standard deviation and Expected Absolute Systematic Error by data source. Select row to view in above plot."
      )
      return(output)
    })

    getCalibrationPlot <- reactive({
      s <- selectedExposureOutcome()
      treatment <- s$TARGET_COHORT_ID
      outcome <- s$OUTCOME_COHORT_ID

      plot <- ggplot2::ggplot()
      if (!is.na(treatment)) {
        null <- getNullDist()
        selectedRows <- input$nullDistribution_rows_selected
        validSourceIds <- null[selectedRows,]$SOURCE_ID

        if (length(validSourceIds) == 0) {
          validSourceIds <- dataSources$SOURCE_ID[1]
        }

        negatives <- getNegativeControlSubset(treatment, outcome, dataSources$SOURCE_ID)
        negatives <- negatives[negatives$SOURCE_ID %in% validSourceIds,]

        if (length(negatives)) {
          plotNegatives <- negatives[negatives$RR > 0, ]

          plot <- EmpiricalCalibration::plotCalibrationEffect(logRrNegatives = log(plotNegatives$RR),
                                                              seLogRrNegatives = plotNegatives$SE_LOG_RR)
        } else {
          ParallelLogger::logWarn("Error finding negative controls for treatment", treatment, " and outome", outcome)
        }
      }
      return(plot)
    })

    output$calibrationPlot <- plotly::renderPlotly({
      plot <- getCalibrationPlot()
      return(plotly::ggplotly(plot))
    })

    output$downloadCalibrationPlot <- downloadHandler(
      filename = function() {
        s <- selectedExposureOutcome()
        treatment <- s$TARGET_COHORT_ID
        outcome <- s$OUTCOME_COHORT_ID
        paste0(model$schemaName, '-calibration-plot-', treatment, "-", outcome, '.png')
      },
      content = function(file) {
        ggplot2::ggsave(file, plot = getCalibrationPlot(), device = "png")
      }
    )
  })

  return(server)
}