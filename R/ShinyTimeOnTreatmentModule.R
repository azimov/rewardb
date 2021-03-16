timeOnTreatmentServer <- function(id, model, selectedExposureOutcome) {

  server <- moduleServer(id, function(input, output, session) {
    getTimeToTreatmentStats <- reactive({
      s <- selectedExposureOutcome()
      treatment <- s$TARGET_COHORT_ID
      outcome <- s$OUTCOME_COHORT_ID

      data <- model$getTimeOnTreatmentStats(treatment, outcome)

      return(data)
    })


    output$timeOnTreatmentStats <- DT::renderDataTable({
      data <- getTimeToTreatmentStats()

      if (nrow(data) == 0) {
        return (data.frame())
      }

      output <- DT::datatable(
        data,
        colnames = c("Source", "Mean", "sd", "Min", "P10", "P25", "Median", "P75", "P90", "Max"),
        options = list(dom = 't', columnDefs = list(list(visible = FALSE, targets = c(0)))),
        caption = "Table: shows time on treatment distibution in days for cohort across databases."
      )
      return(output)
    })

    output$timeOnTreatmentDist <- plotly::renderPlotly({
      dt <- getTimeToTreatmentStats()
      plot <- boxPlotDist(dt)
      return(plotly::ggplotly(plot))
    })

  })

  return(server)
}

timeOnTreatmentUi <- function(id) {
  tagList(
    shinycssloaders::withSpinner(plotly::plotlyOutput(NS(id, "timeOnTreatmentDist"))),
    shinycssloaders::withSpinner(DT::dataTableOutput(NS(id, "timeOnTreatmentStats")))
  )
}

timeToOutcomeServer <- function(id, model, selectedExposureOutcome) {
  server <- moduleServer(id, function(input, output, session) {
    getTimeToOutcomeStats <- reactive({
      s <- selectedExposureOutcome()
      treatment <- s$TARGET_COHORT_ID
      outcome <- s$OUTCOME_COHORT_ID

      data <- model$getTimeToOutcomeStats(treatment, outcome)
      return(data)
    })

    output$timeToOutcomeStats <- DT::renderDataTable({
      data <- getTimeToOutcomeStats()

      if (nrow(data) == 0) {
        return (data.frame())
      }

      output <- DT::datatable(
        data,
        colnames = c("Source", "Mean", "sd", "Min", "P10", "P25", "Median", "P75", "P90", "Max"),
        options = list(dom = 't', columnDefs = list(list(visible = FALSE, targets = c(0)))),
        caption = "Table: shows time to outcome distribution measaured in days between exposure and cohort across different databases."
      )
      return(output)
    })

    output$timeToOutcomeDist <- plotly::renderPlotly({
      dt <- getTimeToOutcomeStats()
      plot <- boxPlotDist(dt)
      return(plotly::ggplotly(plot))
    })
  })

  return(server)
}

timeToOutcomeUi <- function(id) {
  tagList(
    shinycssloaders::withSpinner(plotly::plotlyOutput(NS(id, "timeToOutcomeDist"))),
    shinycssloaders::withSpinner(DT::dataTableOutput(NS(id, "timeToOutcomeStats")))
  )
}
