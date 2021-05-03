# Title     : Calibration plot module
# Objective : To provide a calibration plot and module
# Created by: jpg
# Created on: 2020-12-14

CONST_CALIBRATION_PLOT_TXT <- "Plot of calibration of effect estimates. Blue dots are negative controls, yellow diamonds are uncalibrated effect estimates"

calibrationPlotUi <- function(id, figureTitle = "Figure.", figureText = CONST_CALIBRATION_PLOT_TXT) {
  tagList(
    withSpinner(plotly::plotlyOutput(NS(id, "calibrationPlot"), height = 500)),
    div(
      strong(figureTitle),
      paste(figureText),
      downloadButton(NS(id, "downloadCalibrationPlot"), "Save")
    ),
    withSpinner(DT::dataTableOutput(NS(id, "nullDistribution")))
  )
}

calibrationPlotServer <- function(id, model, selectedExposureOutcome, useExposureControls) {

  server <- moduleServer(id, function(input, output, session) {
    ParallelLogger::logInfo("Initialized calibration plot module for: ", model$schemaName)

    dataSources <- model$getDataSources()

    getNegativeControlSubset <- function(treatment, outcome, sourceIds) {
      if (useExposureControls) {
        negatives <- model$getExposureControls(outcomeIds = outcome, sourceIds = sourceIds)
      } else {
        otype <- if (model$getOutcomeType(outcome) == 1) 1 else 0
        negatives <- model$getOutcomeControls(targetIds = treatment, sourceIds = sourceIds)
        # Subset for outcome types
        negatives <- negatives[negatives$OUTCOME_TYPE == otype,]
      }
      return(negatives)
    }

    getNullDist <- reactive({
      s <- selectedExposureOutcome()
      treatment <- s$TARGET_COHORT_ID
      outcome <- s$OUTCOME_COHORT_ID
      nulls <- data.frame()
      if (length(treatment) & length(outcome)) {
        negatives <- getNegativeControlSubset(treatment, outcome, s$usedDataSources)
        for (source in unique(negatives$SOURCE_ID)) {
          subset <- negatives[negatives$SOURCE_ID == source,]
          null <- EmpiricalCalibration::fitNull(log(subset$RR), subset$SE_LOG_RR)
          df <- data.frame(
            "SOURCE_ID" = source,
            "Controls used" = nrow(subset),
            "mean" = round(exp(null[["mean"]]), 3),
            "sd" = round(exp(null[["sd"]]), 3)
          )
          nulls <- rbind(nulls, df)
        }
        nulls <- inner_join(dataSources, nulls, by = "SOURCE_ID")
      }
      return(nulls)
    })

    output$nullDistribution <- DT::renderDataTable({
      null <- getNullDist()
      output <- DT::datatable(
        null,
        options = list(dom = 't', columnDefs = list(list(visible = FALSE, targets = c(0)))),
        rownames = FALSE,
        colnames = c("Source", "N controls", "Mean", "Stdev"),
        caption = "Table: null distribution mean and standaard deviation by data source. Select rows to filter in above plot."
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
          validSourceIds <- dataSources$SOURCE_ID
        }

        positives <- model$getExposureOutcomeRows(treatment, outcome, calibrated = 0)
        positives <- positives[positives$SOURCE_ID %in% validSourceIds,]

        negatives <- getNegativeControlSubset(treatment, outcome, s$usedDataSources)
        negatives <- negatives[negatives$SOURCE_ID %in% validSourceIds,]

        if (length(negatives)) {

          plotPositives <- positives[positives$RR > 0, ]
          plotNegatives <- negatives[negatives$RR > 0, ]

          plot <- EmpiricalCalibration::plotCalibrationEffect(
            logRrNegatives = log(plotNegatives$RR),
            seLogRrNegatives = plotNegatives$SE_LOG_RR,
            logRrPositives = log(plotPositives$RR),
            seLogRrPositives = plotPositives$SE_LOG_RR
          )

          if (min(plotNegatives$RR) < 0.25) {
            # TODO submit a patch to EmpiricalCalibration package
            suppressWarnings({
              breaks <- c(0.0, 0.125, 0.25, 0.5, 1, 2, 4, 6, 8, 10)
              plot <- plot +
                ggplot2::scale_x_continuous("Relative Risk", trans = "log10", limits = c(min(plotNegatives$RR), 10), breaks = breaks, labels = breaks) +
                ggplot2::geom_vline(xintercept = breaks, colour = "#AAAAAA", lty = 1, size = 0.5)
            })
          }
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